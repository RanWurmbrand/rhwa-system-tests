package tests

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/medik8s/system-tests/tests/internal/helpers"
	. "github.com/medik8s/system-tests/tests/internal/medik8sinittools"
	"github.com/medik8s/system-tests/tests/internal/medik8sparams"
	"github.com/medik8s/system-tests/tests/sbr-operator/internal/sbrparams"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// recordSBRCR starts recording the StorageBasedRemediation CR named after nodeName and stops it when
// the spec ends. Call it before the disruption: NHC deletes the CR as soon as the node's
// SBRStorageUnhealthy condition cycles to Unknown, so a poll that starts afterwards can miss the
// whole remediation. The CR may be deleted and recreated during one remediation; every incarnation
// is recorded.
func recordSBRCR(nodeName string) *helpers.ObjectRecorder[*unstructured.Unstructured] {
	GinkgoHelper()
	crClient := APIClient.Resource(schema.GroupVersionResource{
		Group:    sbrparams.CRDGroup,
		Version:  sbrparams.CRDVersion,
		Resource: sbrparams.SBRCRResource,
	}).Namespace(medik8sparams.OperatorNs)
	selector := metav1.ListOptions{FieldSelector: "metadata.name=" + nodeName}

	return startSpecRecorder(helpers.RecorderConfig[*unstructured.Unstructured]{
		Description: "StorageBasedRemediation/" + nodeName,
		List: func(ctx context.Context) ([]*unstructured.Unstructured, string, error) {
			crList, err := crClient.List(ctx, selector)
			if err != nil {
				return nil, "", err
			}

			items := make([]*unstructured.Unstructured, 0, len(crList.Items))
			for crIdx := range crList.Items {
				items = append(items, &crList.Items[crIdx])
			}

			return items, crList.GetResourceVersion(), nil
		},
		Watch: func(ctx context.Context, resourceVersion string) (watch.Interface, error) {
			opts := selector
			opts.ResourceVersion = resourceVersion

			return crClient.Watch(ctx, opts)
		},
	})
}

// recordNode starts recording the Node nodeName and stops it when the spec ends. It uses the
// high-QPS EventsClient so the watch does not compete with the spec's own reads on APIClient.
func recordNode(nodeName string) *helpers.ObjectRecorder[*corev1.Node] {
	GinkgoHelper()
	nodes := EventsClient.CoreV1().Nodes()
	selector := metav1.ListOptions{FieldSelector: "metadata.name=" + nodeName}

	return startSpecRecorder(helpers.RecorderConfig[*corev1.Node]{
		Description: "Node/" + nodeName,
		List: func(ctx context.Context) ([]*corev1.Node, string, error) {
			nodeList, err := nodes.List(ctx, selector)
			if err != nil {
				return nil, "", err
			}

			items := make([]*corev1.Node, 0, len(nodeList.Items))
			for nodeIdx := range nodeList.Items {
				items = append(items, &nodeList.Items[nodeIdx])
			}

			return items, nodeList.GetResourceVersion(), nil
		},
		Watch: func(ctx context.Context, resourceVersion string) (watch.Interface, error) {
			opts := selector
			opts.ResourceVersion = resourceVersion

			return nodes.Watch(ctx, opts)
		},
	})
}

// startSpecRecorder starts a recorder with the suite's retry interval and logging, and registers its
// Stop for the end of the spec. Watch interruptions become a report entry rather than a failure: the
// recorder retries them, and the spec's own assertions decide the outcome.
func startSpecRecorder[T runtime.Object](cfg helpers.RecorderConfig[T]) *helpers.ObjectRecorder[T] {
	GinkgoHelper()
	cfg.RetryInterval = sbrparams.WatchRetryInterval
	cfg.StartTimeout = sbrparams.WatchStartTimeout
	cfg.Logf = GinkgoWriter.Printf
	rec, err := helpers.StartObjectRecorder(cfg)
	Expect(err).ToNot(HaveOccurred(), "Must start recording %s", cfg.Description)

	DeferCleanup(func() {
		rec.Stop()

		if watchErrs := rec.WatchErrors(); len(watchErrs) > 0 {
			AddReportEntry("watch-errors "+rec.Description(), strings.Join(watchErrs, "; "))
		}
	})

	return rec
}

// remediationFenced reports whether a StorageBasedRemediation CR has proven fencing: both
// FencingSucceeded=True and Succeeded=True.
func remediationFenced(sbrCR *unstructured.Unstructured) bool {
	return conditionTrue(sbrCR, sbrparams.FencingSucceededCondition) &&
		conditionTrue(sbrCR, sbrparams.SucceededCondition)
}
