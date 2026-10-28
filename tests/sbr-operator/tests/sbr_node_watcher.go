package tests

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"

	. "github.com/medik8s/system-tests/tests/internal/medik8sinittools"
	"github.com/medik8s/system-tests/tests/internal/medik8sparams"
	"github.com/medik8s/system-tests/tests/sbr-operator/internal/sbrparams"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// sbrWatchState is everything the background watches record. Guarded by sbrNodeWatcher.mu.
type sbrWatchState struct {
	crCreated   bool
	crExists    bool
	crUIDs      []string
	fenced      bool
	fencedCRUID string
	taintSeen   bool
	watchErrs   []string
}

// sbrNodeWatcher follows, in the background, every StorageBasedRemediation CR created for one node
// and that node's out-of-service taint.
//
// Both signals are edge-triggered and short-lived. NHC deletes the CR as soon as the node's
// SBRStorageUnhealthy condition cycles to Unknown, which the agent does after a short stale period,
// and the reconciler removes the taint when the CR goes away. A poll loop that only starts after the
// disruption can therefore miss the whole remediation and report a false failure. Starting the
// watches before the disruption and reading their results afterwards removes that race.
//
// The watches only record; they never assert. Gomega must run on the spec's own goroutine, so the
// accessors below are written as Eventually bodies for the spec to call.
type sbrNodeWatcher struct {
	nodeName string
	mu       sync.Mutex
	state    sbrWatchState
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// startSBRNodeWatcher starts the CR and Node watches for nodeName and returns once both are
// running. Call it before injecting the disruption, and stop it with DeferCleanup(watcher.stop).
func startSBRNodeWatcher(nodeName string) *sbrNodeWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	watcher := &sbrNodeWatcher{nodeName: nodeName, cancel: cancel}

	watcher.wg.Add(2)

	go watcher.followRemediationCRs(ctx)
	go watcher.followNode(ctx)

	GinkgoWriter.Printf("Watching StorageBasedRemediation CRs and the %s taint of node %s\n",
		sbrparams.OutOfServiceTaintKey, nodeName)

	return watcher
}

// stop cancels both watches, waits for them to finish, and reports what they saw. Any watch error
// becomes a report entry rather than a failure: a dropped watch is retried, and the spec's own
// assertions decide the outcome.
func (w *sbrNodeWatcher) stop() {
	w.cancel()
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	GinkgoWriter.Printf("Node %s watch summary: StorageBasedRemediation CRs observed %v, "+
		"fencing proven on %q, %s taint observed=%t\n",
		w.nodeName, w.state.crUIDs, w.state.fencedCRUID,
		sbrparams.OutOfServiceTaintKey, w.state.taintSeen)

	if len(w.state.watchErrs) > 0 {
		AddReportEntry("sbr-node-watch-errors", strings.Join(w.state.watchErrs, "; "))
	}
}

// followRemediationCRs watches the StorageBasedRemediation CR named after the node. The CR is a
// custom resource, so it goes through the dynamic client rather than a typed one.
func (w *sbrNodeWatcher) followRemediationCRs(ctx context.Context) {
	defer GinkgoRecover()
	defer w.wg.Done()

	crClient := APIClient.Resource(schema.GroupVersionResource{
		Group:    sbrparams.CRDGroup,
		Version:  sbrparams.CRDVersion,
		Resource: sbrparams.SBRCRResource,
	}).Namespace(medik8sparams.OperatorNs)
	selector := metav1.ListOptions{FieldSelector: "metadata.name=" + w.nodeName}

	w.follow(ctx, "StorageBasedRemediation/"+w.nodeName,
		func(ctx context.Context) (string, error) {
			crList, err := crClient.List(ctx, selector)
			if err != nil {
				return "", fmt.Errorf("list: %w", err)
			}

			w.mu.Lock()
			w.state.crExists = false
			w.mu.Unlock()

			for crIdx := range crList.Items {
				w.recordRemediationCR(&crList.Items[crIdx])
			}

			return crList.GetResourceVersion(), nil
		},
		func(ctx context.Context, resourceVersion string) (watch.Interface, error) {
			opts := selector
			opts.ResourceVersion = resourceVersion

			return crClient.Watch(ctx, opts)
		},
		w.handleRemediationEvent,
	)
}

// followNode watches the victim Node for the out-of-service taint. It uses the high-QPS
// EventsClient so the watch does not compete with the spec's own reads on the throttled APIClient.
func (w *sbrNodeWatcher) followNode(ctx context.Context) {
	defer GinkgoRecover()
	defer w.wg.Done()

	nodes := EventsClient.CoreV1().Nodes()
	selector := metav1.ListOptions{FieldSelector: "metadata.name=" + w.nodeName}

	w.follow(ctx, "Node/"+w.nodeName,
		func(ctx context.Context) (string, error) {
			nodeList, err := nodes.List(ctx, selector)
			if err != nil {
				return "", fmt.Errorf("list: %w", err)
			}

			for nodeIdx := range nodeList.Items {
				w.recordNode(&nodeList.Items[nodeIdx])
			}

			return nodeList.GetResourceVersion(), nil
		},
		func(ctx context.Context, resourceVersion string) (watch.Interface, error) {
			opts := selector
			opts.ResourceVersion = resourceVersion

			return nodes.Watch(ctx, opts)
		},
		w.handleNodeEvent,
	)
}

// follow keeps one watch alive for the lifetime of ctx. It seeds the state with a List, watches
// from that List's resourceVersion, and starts over whenever the watch ends, so a closed connection
// or an expired resourceVersion resumes instead of silently going deaf. Re-listing on every restart
// also recovers any state that changed while no watch was established.
func (w *sbrNodeWatcher) follow(
	ctx context.Context,
	what string,
	seed func(context.Context) (string, error),
	open func(context.Context, string) (watch.Interface, error),
	handle func(watch.Event),
) {
	for ctx.Err() == nil {
		resourceVersion, seedErr := seed(ctx)
		if seedErr != nil {
			w.recordWatchErr(ctx, what, seedErr)

			continue
		}

		watcher, openErr := open(ctx, resourceVersion)
		if openErr != nil {
			w.recordWatchErr(ctx, what, fmt.Errorf("watch: %w", openErr))

			continue
		}

		for event := range watcher.ResultChan() {
			if event.Type == watch.Error {
				w.recordWatchErr(ctx, what, fmt.Errorf("watch event: %v", event.Object))

				break
			}

			handle(event)
		}

		watcher.Stop()
	}
}

// recordWatchErr notes a watch failure and backs off before the caller retries. A cancelled context
// is the normal way the watches end, so it is neither recorded nor waited out.
func (w *sbrNodeWatcher) recordWatchErr(ctx context.Context, what string, err error) {
	if ctx.Err() != nil {
		return
	}

	w.mu.Lock()
	w.state.watchErrs = append(w.state.watchErrs, fmt.Sprintf("%s: %v", what, err))
	w.mu.Unlock()

	GinkgoWriter.Printf("Warning: watch on %s interrupted, retrying in %s: %v\n",
		what, sbrparams.WatchRetryInterval, err)

	timer := time.NewTimer(sbrparams.WatchRetryInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// handleRemediationEvent folds one StorageBasedRemediation watch event into the recorded state.
func (w *sbrNodeWatcher) handleRemediationEvent(event watch.Event) {
	sbrCR, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		return
	}

	switch event.Type {
	case watch.Added, watch.Modified:
		w.recordRemediationCR(sbrCR)
	case watch.Deleted:
		w.mu.Lock()
		w.state.crExists = false
		w.mu.Unlock()
	case watch.Bookmark, watch.Error:
	default:
	}
}

// recordRemediationCR records that a CR exists and whether it has proven fencing. Fencing counts on
// any CR for this node, not only the first: NHC legitimately deletes and recreates the CR when the
// node's SBRStorageUnhealthy condition cycles to Unknown and back, and the replacement is the one
// that finishes the job.
func (w *sbrNodeWatcher) recordRemediationCR(sbrCR *unstructured.Unstructured) {
	uid := string(sbrCR.GetUID())
	fenced := conditionTrue(sbrCR, sbrparams.FencingSucceededCondition) &&
		conditionTrue(sbrCR, sbrparams.SucceededCondition)

	w.mu.Lock()
	defer w.mu.Unlock()

	w.state.crCreated = true
	w.state.crExists = true

	if !slices.Contains(w.state.crUIDs, uid) {
		w.state.crUIDs = append(w.state.crUIDs, uid)
		GinkgoWriter.Printf("Observed StorageBasedRemediation/%s (uid %s)\n", w.nodeName, uid)
	}

	if fenced && !w.state.fenced {
		w.state.fenced = true
		w.state.fencedCRUID = uid
		GinkgoWriter.Printf("Observed %s=True and %s=True on StorageBasedRemediation/%s (uid %s)\n",
			sbrparams.FencingSucceededCondition, sbrparams.SucceededCondition, w.nodeName, uid)
	}
}

// handleNodeEvent folds one Node watch event into the recorded state.
func (w *sbrNodeWatcher) handleNodeEvent(event watch.Event) {
	node, ok := event.Object.(*corev1.Node)
	if !ok {
		return
	}

	switch event.Type {
	case watch.Added, watch.Modified:
		w.recordNode(node)
	case watch.Deleted, watch.Bookmark, watch.Error:
	default:
	}
}

// recordNode records whether the node has ever carried the out-of-service taint. Only the sighting
// is kept: the reconciler removes the taint again when the CR is deleted, so the live node object is
// not evidence that fencing ever tainted it.
func (w *sbrNodeWatcher) recordNode(node *corev1.Node) {
	if !hasOutOfServiceTaint(node) {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.state.taintSeen {
		w.state.taintSeen = true
		GinkgoWriter.Printf("Observed the %s taint on node %s\n", sbrparams.OutOfServiceTaintKey, w.nodeName)
	}
}

// remediationCreated is an Eventually body: nil once NHC has created a StorageBasedRemediation CR
// for the node.
func (w *sbrNodeWatcher) remediationCreated() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state.crCreated {
		return nil
	}

	return fmt.Errorf("no StorageBasedRemediation/%s observed yet", w.nodeName)
}

// fencingProven is an Eventually body: nil once a StorageBasedRemediation CR for the node has
// reported both FencingSucceeded=True and Succeeded=True. It stays nil afterwards even though NHC
// later deletes that CR.
func (w *sbrNodeWatcher) fencingProven() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state.fenced {
		return nil
	}

	if !w.state.crCreated {
		return fmt.Errorf("no StorageBasedRemediation/%s observed yet", w.nodeName)
	}

	return fmt.Errorf("StorageBasedRemediation/%s observed (uids %v) but %s=True and %s=True were not",
		w.nodeName, w.state.crUIDs, sbrparams.FencingSucceededCondition, sbrparams.SucceededCondition)
}

// outOfServiceTainted is an Eventually body: nil once the node was seen carrying the out-of-service
// taint at any point during the remediation.
func (w *sbrNodeWatcher) outOfServiceTainted() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.state.taintSeen {
		return nil
	}

	return fmt.Errorf("node %s was never observed with the %s taint", w.nodeName, sbrparams.OutOfServiceTaintKey)
}

// remediationRemoved is an Eventually body: nil once a CR existed for the node and none exists any
// more, which is how NHC closes out a finished remediation.
func (w *sbrNodeWatcher) remediationRemoved() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.state.crCreated {
		return fmt.Errorf("no StorageBasedRemediation/%s was ever observed", w.nodeName)
	}

	if w.state.crExists {
		return fmt.Errorf("StorageBasedRemediation/%s still exists; waiting for NHC cleanup", w.nodeName)
	}

	return nil
}
