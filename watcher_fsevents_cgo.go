// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package notify

/*
#include <CoreServices/CoreServices.h>
#include <dispatch/dispatch.h>

void gostream(uintptr_t, uintptr_t, size_t, uintptr_t, uintptr_t, uintptr_t);
void goPollFire(uintptr_t);

static FSEventStreamRef EventStreamCreate(FSEventStreamContext * context, uintptr_t info, CFArrayRef paths, FSEventStreamEventId since, CFTimeInterval latency, FSEventStreamCreateFlags flags) {
	context->info = (void*) info;
	return FSEventStreamCreate(NULL, (FSEventStreamCallback) gostream, context, paths, since, latency, flags);
}

// dispatchPoll posts h onto the serial queue all streams dispatch on, so a
// poll-mode timer restart is serialized with event dispatch.
static void pollfire(void *ctx) { goPollFire((uintptr_t)ctx); }
static void dispatchPoll(dispatch_queue_t qq, uintptr_t h) { dispatch_async_f(qq, (void*)h, pollfire); }

#cgo LDFLAGS: -framework CoreServices
*/
import "C"

import (
	"errors"
	"os"
	"runtime/cgo"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var nilstream C.FSEventStreamRef

// Default arguments for FSEventStreamCreate function.
var (
	// latency 0 makes fseventsd deliver every micro-batch immediately with
	// no coalescing; on a busy tree (measured ~1000 events/sec on a
	// 2.4M-file home dir, 2026-10-13) that kept fseventsd at >100% CPU
	// sustained and the consumer at ~80%. 1s lets fseventsd coalesce the
	// flood into one delivery per second. Consumers that batch downstream
	// anyway (syncthing's aggregator delays 10s by default) see no change
	// in end-to-end latency. NoDefer still applies, so historical replays
	// (heals, boot replay) are delivered without this delay.
	latency = C.CFTimeInterval(1.0)
	since   = uint64(C.FSEventsGetCurrentEventId())
)

// ignoreSelf gates kFSEventStreamCreateFlagIgnoreSelf at stream creation.
// Default off (upstream semantics: self-writes are delivered). A consumer
// that already knows its own writes (syncthing: its database inside the
// watched tree and every pulled file) opts in via SetIgnoreSelf and skips
// that pure-overhead journaling+delivery. Without the opt-in the flag
// silently breaks any consumer whose own process writes into the watched
// tree - including every in-process watcher test (observed 2026-10-16:
// syncthing's lib/fs watcher tests and this package's own tests all time
// out with the flag unconditional, because the test's writes are
// self-writes).
var ignoreSelf atomic.Bool

// SetIgnoreSelf toggles server-side filtering of events caused by this
// process; affects streams started after the call.
func SetIgnoreSelf(v bool) {
	ignoreSelf.Store(v)
}

// streamFlags computes the FSEventStreamCreate flags at stream creation
// time, honoring SetIgnoreSelf.
func streamFlags() C.FSEventStreamCreateFlags {
	f := C.FSEventStreamCreateFlags(C.kFSEventStreamCreateFlagFileEvents | C.kFSEventStreamCreateFlagNoDefer)
	if ignoreSelf.Load() {
		f |= C.kFSEventStreamCreateFlagIgnoreSelf
	}
	return f
}

// replayHistorical gates processing of events that arrive before
// FSEventsHistoryDone. Those are historical journal events, delivered when a
// stream is created with a since-ID in the past; the library default of
// dropping them preserves the semantics of Watch (events after the call).
// SetSinceEventID turns processing on, because historical events are exactly
// what a caller seeding a journal position wants.
var replayHistorical atomic.Bool

// SetSinceEventID seeds the journal position used by streams started after
// the call. The seed only ever moves backwards: the minimum of the current
// and given values wins, so concurrent callers (e.g. multiple watched trees
// with their own persisted positions) cannot accidentally raise each other's
// position and lose events. It also enables processing of historical events
// (see replayHistorical). Call before starting the first stream; streams read
// the seed when they are started.
func SetSinceEventID(id uint64) {
	if id == 0 {
		return
	}
	for {
		cur := atomic.LoadUint64(&since)
		if id >= cur || atomic.CompareAndSwapUint64(&since, cur, id) {
			break
		}
	}
	replayHistorical.Store(true)
}

// ReplayEnabled reports whether historical event processing is currently
// armed, i.e. whether a live drop flag will be healed by journal replay
// rather than forwarded to the consumer for a full rescan.
func ReplayEnabled() bool {
	return replayHistorical.Load()
}

// ResetSinceEventID abandons journal replay: the seed is raised to the
// current event ID and historical events are dropped again. Callers use this
// when the journal turns out not to cover the seeded position (signalled by
// FSEventsMustScanSubDirs and friends), to prevent a restarted stream from
// replaying the same failure in a loop.
func ResetSinceEventID() {
	replayHistorical.Store(false)
	atomic.StoreUint64(&since, uint64(C.FSEventsGetCurrentEventId()))
}

// CurrentEventID returns the current event ID of the FSEvents journal, i.e.
// the ID the next recorded event will get. A persisted seed greater than this
// means the journal was reset (e.g. the volume was reformatted) and the seed
// is meaningless.
func CurrentEventID() uint64 {
	return uint64(C.FSEventsGetCurrentEventId())
}

// global dispatch queue which all streams are registered with
var q C.dispatch_queue_t = C.dispatch_queue_create(
	C.CString("com.github.rjeczalik.notify"),
	(C.dispatch_queue_attr_t)(C.DISPATCH_QUEUE_SERIAL),
)

// Errors returned when FSEvents functions fail.
var (
	errCreate = os.NewSyscallError("FSEventStreamCreate", errors.New("NULL"))
	errStart  = os.NewSyscallError("FSEventStreamStart", errors.New("false"))
)

//export goPollFire
func goPollFire(h C.uintptr_t) {
	handle := cgo.Handle(h)
	fn := handle.Value().(func())
	handle.Delete()
	fn()
}

// schedulePollRestart restarts w's stream from fromID after d, serialized
// with event dispatch: the timer only posts onto the serial queue, and the
// queued closure checks it was not superseded by a newer poll or a Stop.
func (w *watch) schedulePollRestart(fromID uint64, d time.Duration) {
	w.pollGen++
	gen := w.pollGen
	time.AfterFunc(d, func() {
		h := cgo.NewHandle(func() {
			if gen != w.pollGen || atomic.LoadUint32(&w.events) == 0 {
				return
			}
			w.live = false
			w.replaying = true
			w.delivered = false
			w.hasPending = false
			w.pendingCount = 0
			if err := w.restartStream(fromID); err != nil {
				w.healDisabled = true
				healprintf("poll restart FAILED path=%s from=%d: %v\n", w.path, fromID, err)
				// The poll chain is dead; make the consumer full-scan, its
				// completion re-arms and restarts the watch.
				select {
				case w.c <- &event{fse: FSEvent{Path: w.path, Flags: uint32(FSEventsMustScanSubDirs)}, event: FSEventsMustScanSubDirs}:
				default:
				}
			}
		})
		C.dispatchPoll(q, C.uintptr_t(h))
	})
}

//export gostream
func gostream(_, info uintptr, n C.size_t, paths, flags, ids uintptr) {
	const (
		offchar = unsafe.Sizeof((*C.char)(nil))
		offflag = unsafe.Sizeof(C.FSEventStreamEventFlags(0))
		offid   = unsafe.Sizeof(C.FSEventStreamEventId(0))
	)
	if n == 0 {
		return
	}
	fn := streamFuncs.get(info)
	if fn == nil {
		return
	}
	ev := make([]FSEvent, 0, int(n))
	for i := uintptr(0); i < uintptr(n); i++ {
		switch flags := *(*uint32)(unsafe.Pointer((flags + i*offflag))); {
		case flags&uint32(FSEventsEventIdsWrapped) != 0:
			atomic.StoreUint64(&since, uint64(C.FSEventsGetCurrentEventId()))
			// Surface the wrap as an event: it means journal positions are no
			// longer comparable, which a replaying consumer must treat as a
			// journal failure and fall back to a full scan.
			ev = append(ev, FSEvent{
				Path:  C.GoString(*(**C.char)(unsafe.Pointer(paths + i*offchar))),
				Flags: flags,
				ID:    *(*uint64)(unsafe.Pointer(ids + i*offid)),
			})
		default:
			ev = append(ev, FSEvent{
				Path:  C.GoString(*(**C.char)(unsafe.Pointer(paths + i*offchar))),
				Flags: flags,
				ID:    *(*uint64)(unsafe.Pointer(ids + i*offid)),
			})
		}
	}
	fn(ev)
}

// StreamFunc is a callback called when stream receives file events.
type streamFunc func([]FSEvent)

var streamFuncs = streamFuncRegistry{m: map[uintptr]streamFunc{}}

type streamFuncRegistry struct {
	mu sync.Mutex
	m  map[uintptr]streamFunc
	i  uintptr
}

func (r *streamFuncRegistry) get(id uintptr) streamFunc {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[id]
}

func (r *streamFuncRegistry) add(fn streamFunc) uintptr {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.i++
	r.m[r.i] = fn
	return r.i
}

func (r *streamFuncRegistry) delete(id uintptr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, id)
}

// Stream represents a single watch-point which listens for events scheduled on the global dispatch queue.
type stream struct {
	path string
	ref  C.FSEventStreamRef
	info uintptr
	// sinceID, when nonzero, pins the journal position this stream starts
	// from, overriding the package-level seed (used by watch heals, which
	// replay a known gap without disturbing other watches).
	sinceID uint64
}

// NewStream creates a stream for given path, listening for file events and
// calling fn upon receiving any.
func newStream(path string, fn streamFunc) *stream {
	return &stream{
		path: path,
		info: streamFuncs.add(fn),
	}
}

// newStreamSince is newStream with an explicit journal start position.
func newStreamSince(path string, fn streamFunc, sinceID uint64) *stream {
	s := newStream(path, fn)
	s.sinceID = sinceID
	return s
}

// Start creates a FSEventStream for the given path and schedules on the global dispatch queue.
// It's a nop if the stream was already started.
func (s *stream) Start() error {
	if s.ref != nilstream {
		return nil
	}
	p := C.CFStringCreateWithCStringNoCopy(C.kCFAllocatorDefault, C.CString(s.path), C.kCFStringEncodingUTF8, C.kCFAllocatorDefault)
	path := C.CFArrayCreate(C.kCFAllocatorDefault, (*unsafe.Pointer)(unsafe.Pointer(&p)), 1, nil)
	ctx := C.FSEventStreamContext{}
	sinceID := s.sinceID
	if sinceID == 0 {
		sinceID = atomic.LoadUint64(&since)
	}
	ref := C.EventStreamCreate(&ctx, C.uintptr_t(s.info), path, C.FSEventStreamEventId(sinceID), latency, streamFlags())
	if ref == nilstream {
		return errCreate
	}
	C.FSEventStreamSetDispatchQueue(ref, q)
	if C.FSEventStreamStart(ref) == C.Boolean(0) {
		C.FSEventStreamInvalidate(ref)
		return errStart
	}
	s.ref = ref
	return nil
}

// Stop stops underlying FSEventStream and unregisters it from the global dispatch queue.
func (s *stream) Stop() {
	if s.ref == nilstream {
		return
	}
	C.FSEventStreamStop(s.ref)
	C.FSEventStreamInvalidate(s.ref)
	s.ref = nilstream
	streamFuncs.delete(s.info)
}
