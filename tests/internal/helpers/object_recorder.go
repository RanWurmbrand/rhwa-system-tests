package helpers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// RecorderConfig describes the single object an ObjectRecorder follows. List and Watch must both be
// narrowed to that one object, typically with a metadata.name field selector.
type RecorderConfig[T runtime.Object] struct {
	// Description names the object in errors and logs, e.g. "Node/worker-0".
	Description string
	// List returns the object (zero or one item) and the list's resourceVersion.
	List func(ctx context.Context) ([]T, string, error)
	// Watch opens a watch on the object starting at resourceVersion.
	Watch func(ctx context.Context, resourceVersion string) (watch.Interface, error)
	// RetryInterval is how long to back off after a failed list or watch before trying again.
	RetryInterval time.Duration
	// StartTimeout is how long StartObjectRecorder keeps retrying the first list before giving up.
	// Zero means a single attempt.
	StartTimeout time.Duration
	// Logf, if set, receives progress and watch-interruption messages.
	Logf func(format string, args ...any)
}

// ObjectRecorder keeps, in the background, every version of one object seen by a list+watch, so
// short-lived states can be asserted after the fact.
//
// Polling live state during a disruption races the system under test: a condition, taint or CR can
// appear and disappear between two polls, and a check that starts after the disruption can miss it
// entirely. Start a recorder before the disruption and ask it afterwards what it saw.
//
// The recorder only records; it never asserts. Its accessors return Eventually bodies so Gomega
// still runs on the spec's goroutine. Versions that come and go while the watch is being
// re-established are not seen; each interruption is kept in WatchErrors.
type ObjectRecorder[T runtime.Object] struct {
	cfg       RecorderConfig[T]
	mu        sync.Mutex
	seen      []T
	exists    bool
	watchErrs []string
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// StartObjectRecorder lists the object and then follows it in the background. It returns only after
// a list succeeded, so everything from the moment it returns is covered; a failed first list is
// retried until cfg.StartTimeout. Stop it with Stop.
func StartObjectRecorder[T runtime.Object](cfg RecorderConfig[T]) (*ObjectRecorder[T], error) {
	ctx, cancel := context.WithCancel(context.Background())
	rec := &ObjectRecorder[T]{cfg: cfg, cancel: cancel}
	deadline := time.Now().Add(cfg.StartTimeout)

	for {
		resourceVersion, seedErr := rec.seed(ctx)
		if seedErr == nil {
			rec.wg.Add(1)

			go rec.follow(ctx, resourceVersion)

			rec.logf("Recording %s\n", cfg.Description)

			return rec, nil
		}

		if !time.Now().Before(deadline) {
			cancel()

			return nil, fmt.Errorf("%s: initial list did not succeed within %s: %w",
				cfg.Description, cfg.StartTimeout, seedErr)
		}

		rec.recordWatchErr(ctx, fmt.Errorf("initial list: %w", seedErr))
	}
}

// Stop ends the background watch and waits for it to finish.
func (r *ObjectRecorder[T]) Stop() {
	r.cancel()
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logf("Stopped recording %s: %d versions observed, %d watch interruptions\n",
		r.cfg.Description, len(r.seen), len(r.watchErrs))
}

// Description returns the configured description of the recorded object.
func (r *ObjectRecorder[T]) Description() string {
	return r.cfg.Description
}

// WatchErrors returns every list or watch interruption seen so far.
func (r *ObjectRecorder[T]) WatchErrors() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.watchErrs...)
}

// EverExisted returns an Eventually body that is nil once the object has been observed at all.
func (r *ObjectRecorder[T]) EverExisted() func() error {
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		if len(r.seen) > 0 {
			return nil
		}

		return fmt.Errorf("%s has not been observed yet", r.cfg.Description)
	}
}

// EverSaw returns an Eventually body that is nil once any observed version satisfied pred. It stays
// nil afterwards, even if the object later changes or is deleted. what describes pred in the error.
func (r *ObjectRecorder[T]) EverSaw(what string, pred func(T) bool) func() error {
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		for _, obj := range r.seen {
			if pred(obj) {
				return nil
			}
		}

		return fmt.Errorf("%s: never observed %s (%d versions seen)", r.cfg.Description, what, len(r.seen))
	}
}

// NeverSaw returns a body for Consistently or a final check: nil as long as no observed version
// satisfied pred. Unlike a live poll, it also catches a version that came and went between polls.
func (r *ObjectRecorder[T]) NeverSaw(what string, pred func(T) bool) func() error {
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		for idx, obj := range r.seen {
			if pred(obj) {
				return fmt.Errorf("%s: observed %s (version %d of %d)", r.cfg.Description, what, idx+1, len(r.seen))
			}
		}

		return nil
	}
}

// Gone returns an Eventually body that is nil once the object was observed and no longer exists.
func (r *ObjectRecorder[T]) Gone() func() error {
	return func() error {
		r.mu.Lock()
		defer r.mu.Unlock()

		if len(r.seen) == 0 {
			return fmt.Errorf("%s was never observed", r.cfg.Description)
		}

		if r.exists {
			return fmt.Errorf("%s still exists", r.cfg.Description)
		}

		return nil
	}
}

// follow keeps a watch alive for the lifetime of ctx. Whenever the watch ends (closed connection,
// expired resourceVersion, error event) it re-lists, which also recovers state that changed while
// no watch was established, and watches again from the new list.
func (r *ObjectRecorder[T]) follow(ctx context.Context, resourceVersion string) {
	defer r.wg.Done()

	for ctx.Err() == nil {
		if resourceVersion == "" {
			var seedErr error

			resourceVersion, seedErr = r.seed(ctx)
			if seedErr != nil {
				r.recordWatchErr(ctx, fmt.Errorf("list: %w", seedErr))

				continue
			}
		}

		watcher, openErr := r.cfg.Watch(ctx, resourceVersion)
		resourceVersion = ""

		if openErr != nil {
			r.recordWatchErr(ctx, fmt.Errorf("watch: %w", openErr))

			continue
		}

		r.drain(ctx, watcher)
		watcher.Stop()
	}
}

// drain records watch events until the watch ends, reports an error, or ctx is cancelled.
func (r *ObjectRecorder[T]) drain(ctx context.Context, watcher watch.Interface) {
	for {
		var event watch.Event

		select {
		case <-ctx.Done():
			return
		case received, open := <-watcher.ResultChan():
			if !open {
				return
			}

			event = received
		}

		switch event.Type {
		case watch.Added, watch.Modified:
			r.record(event.Object, true)
		case watch.Deleted:
			r.record(event.Object, false)
		case watch.Error:
			r.recordWatchErr(ctx, fmt.Errorf("watch event: %v", event.Object))

			return
		case watch.Bookmark:
		default:
		}
	}
}

// seed lists the object, records what the list returned, and returns the list's resourceVersion.
func (r *ObjectRecorder[T]) seed(ctx context.Context) (string, error) {
	items, resourceVersion, err := r.cfg.List(ctx)
	if err != nil {
		return "", err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.seen = append(r.seen, items...)
	r.exists = len(items) > 0

	return resourceVersion, nil
}

// record appends one watched version and whether the object still exists after it.
func (r *ObjectRecorder[T]) record(obj runtime.Object, exists bool) {
	typed, ok := obj.(T)
	if !ok {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.seen = append(r.seen, typed)
	r.exists = exists
}

// recordWatchErr notes an interruption and backs off before the caller retries. A cancelled context
// is the normal way the recorder ends, so it is neither recorded nor waited out.
func (r *ObjectRecorder[T]) recordWatchErr(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}

	r.mu.Lock()
	r.watchErrs = append(r.watchErrs, err.Error())
	r.mu.Unlock()
	r.logf("Warning: watch on %s interrupted, retrying in %s: %v\n", r.cfg.Description, r.cfg.RetryInterval, err)
	timer := time.NewTimer(r.cfg.RetryInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (r *ObjectRecorder[T]) logf(format string, args ...any) {
	if r.cfg.Logf != nil {
		r.cfg.Logf(format, args...)
	}
}
