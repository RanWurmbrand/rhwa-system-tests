package helpers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// fakeNodeAPI serves a scripted List and hands out one FakeWatcher per Watch call.
type fakeNodeAPI struct {
	mu      sync.Mutex
	listed  []*corev1.Node
	listErr error
	// failLists makes the next failLists List calls fail with listErr.
	failLists int
	watchers  chan *watch.FakeWatcher
}

func newFakeNodeAPI(initial ...*corev1.Node) *fakeNodeAPI {
	return &fakeNodeAPI{listed: initial, watchers: make(chan *watch.FakeWatcher, 10)}
}

func (f *fakeNodeAPI) config() RecorderConfig[*corev1.Node] {
	return RecorderConfig[*corev1.Node]{
		Description: "Node/worker-0",
		List: func(context.Context) ([]*corev1.Node, string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()

			if f.failLists > 0 {
				f.failLists--

				return nil, "", f.listErr
			}

			return f.listed, "1", nil
		},
		Watch: func(context.Context, string) (watch.Interface, error) {
			fakeWatcher := watch.NewFake()
			f.watchers <- fakeWatcher

			return fakeWatcher, nil
		},
		RetryInterval: time.Millisecond,
	}
}

func (f *fakeNodeAPI) setListed(nodes ...*corev1.Node) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.listed = nodes
}

func (f *fakeNodeAPI) nextWatcher(t *testing.T) *watch.FakeWatcher {
	t.Helper()

	select {
	case fakeWatcher := <-f.watchers:
		return fakeWatcher
	case <-time.After(5 * time.Second):
		t.Fatal("recorder did not open a watch")

		return nil
	}
}

func testNode(tainted bool) *corev1.Node {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-0"}}
	if tainted {
		node.Spec.Taints = []corev1.Taint{{Key: corev1.TaintNodeOutOfService}}
	}

	return node
}

func isTainted(node *corev1.Node) bool {
	return len(node.Spec.Taints) > 0
}

// waitFor polls check until it returns nil, failing the test after a few seconds.
func waitFor(t *testing.T, check func() error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := check()
		if err == nil {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("condition not met: %v", err)
		}

		time.Sleep(time.Millisecond)
	}
}

func TestObjectRecorderKeepsShortLivedState(t *testing.T) {
	api := newFakeNodeAPI(testNode(false))
	rec, err := StartObjectRecorder(api.config())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rec.Stop()

	if err := rec.EverSaw("taint", isTainted)(); err == nil {
		t.Fatal("EverSaw passed before the taint was ever applied")
	}

	// The taint appears and is removed again before anyone checks: a live read would miss it.
	fakeWatcher := api.nextWatcher(t)
	fakeWatcher.Modify(testNode(true))
	fakeWatcher.Modify(testNode(false))
	waitFor(t, rec.EverSaw("taint", isTainted))

	if err := rec.NeverSaw("taint", isTainted)(); err == nil {
		t.Fatal("NeverSaw passed although the taint was observed")
	}
}

func TestObjectRecorderGone(t *testing.T) {
	api := newFakeNodeAPI()
	rec, err := StartObjectRecorder(api.config())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rec.Stop()

	if err := rec.Gone()(); err == nil {
		t.Fatal("Gone passed for an object that was never observed")
	}

	fakeWatcher := api.nextWatcher(t)
	fakeWatcher.Add(testNode(false))
	waitFor(t, rec.EverExisted())

	if err := rec.Gone()(); err == nil {
		t.Fatal("Gone passed while the object still exists")
	}

	fakeWatcher.Delete(testNode(false))
	waitFor(t, rec.Gone())
}

func TestObjectRecorderRelistsAfterWatchCloses(t *testing.T) {
	api := newFakeNodeAPI(testNode(false))
	rec, err := StartObjectRecorder(api.config())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rec.Stop()

	// The taint lands while no watch is open; the re-list after the drop must pick it up.
	api.setListed(testNode(true))
	api.nextWatcher(t).Stop()
	api.nextWatcher(t)
	waitFor(t, rec.EverSaw("taint", isTainted))
}

func TestObjectRecorderRecordsWatchErrorEvents(t *testing.T) {
	api := newFakeNodeAPI(testNode(false))
	rec, err := StartObjectRecorder(api.config())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer rec.Stop()

	api.nextWatcher(t).Error(&metav1.Status{Message: "too old resource version"})
	api.nextWatcher(t)

	if errs := rec.WatchErrors(); len(errs) != 1 {
		t.Fatalf("expected one recorded watch error, got %v", errs)
	}
}

func TestStartObjectRecorderRetriesInitialList(t *testing.T) {
	api := newFakeNodeAPI(testNode(false))
	api.listErr = errors.New("connection refused")
	api.failLists = 2
	cfg := api.config()
	cfg.StartTimeout = 5 * time.Second
	rec, err := StartObjectRecorder(cfg)
	if err != nil {
		t.Fatalf("expected StartObjectRecorder to retry past transient list failures: %v", err)
	}
	defer rec.Stop()

	if errs := rec.WatchErrors(); len(errs) != 2 {
		t.Fatalf("expected the two failed lists to be recorded, got %v", errs)
	}
}

func TestStartObjectRecorderGivesUpAfterStartTimeout(t *testing.T) {
	api := newFakeNodeAPI()
	api.listErr = errors.New("connection refused")
	api.failLists = 1000
	cfg := api.config()
	cfg.StartTimeout = 20 * time.Millisecond

	if _, err := StartObjectRecorder(cfg); err == nil {
		t.Fatal("expected StartObjectRecorder to fail when no list succeeds within StartTimeout")
	}
}
