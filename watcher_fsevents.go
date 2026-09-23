// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package notify

import (
	"errors"
	"strings"
	"sync/atomic"
)

const (
	failure = uint32(FSEventsMustScanSubDirs | FSEventsUserDropped | FSEventsKernelDropped)
	filter  = uint32(FSEventsCreated | FSEventsRemoved | FSEventsRenamed |
		FSEventsModified | FSEventsInodeMetaMod)
)

// FSEvent represents single file event. It is created out of values passed by
// FSEvents to FSEventStreamCallback function.
type FSEvent struct {
	Path  string // real path of the file or directory
	ID    uint64 // ID of the event (FSEventStreamEventId)
	Flags uint32 // joint FSEvents* flags (FSEventStreamEventFlags)
}

// splitflags separates event flags from single set into slice of flags.
func splitflags(set uint32) (e []uint32) {
	for i := uint32(1); set != 0; i, set = i<<1, set>>1 {
		if (set & 1) != 0 {
			e = append(e, i)
		}
	}
	return
}

// historyLen is the number of paths in each of the two generations of a
// watch's history, so a watch remembers at most 2*historyLen paths.
//
// fseventsd merges the flags of all events for a path into one in-memory
// record, and repeats them in every later event for that path until it writes
// the record out. It writes its records out once it has collected 2730 of them
// (measured on macOS 26), and often sooner. Once historyLen other paths have
// changed since a path last did, fseventsd no longer repeats old flags for it,
// so strip can forget the path.
const historyLen = 8192

// history remembers the last Created or Removed state that strip saw for each
// path. New states go into the current generation. When that is full, it
// becomes the previous generation and the one before it is dropped, so a path
// is remembered until at least historyLen other paths have been recorded after
// its last event.
type history struct {
	cur, old map[string]uint32
}

func (h *history) get(path string) uint32 {
	if state, ok := h.cur[path]; ok {
		return state
	}
	return h.old[path]
}

func (h *history) set(path string, state uint32) {
	s, ok := h.cur[path]
	if ok && s == state {
		return
	}
	if !ok && len(h.cur) >= historyLen {
		h.old, h.cur = h.cur, nil
	}
	if h.cur == nil {
		h.cur = make(map[string]uint32)
	}
	// Copy the path, which usually shares memory with the full event path
	// and would otherwise keep all of it allocated.
	h.cur[string([]byte(path))] = state
}

// watch represents a filesystem watchpoint. It is a higher level abstraction
// over FSEvents' stream, which implements filtering of file events based
// on path and event set. It emulates non-recursive watch-point by filtering out
// events which paths are more than 1 level deeper than the watched path.
type watch struct {
	// prev stores the Created or Removed state of recently changed paths, in
	// order to filter out the old flags FSEvents repeats in later events.
	prev    history
	c       chan<- EventInfo
	stream  *stream
	path    string
	events  uint32
	isrec   int32
	flushed bool
}

// Example format:
//
//	~ $ (trigger command) # (event set) -> (effective event set)
//
// Heuristics:
//
// 1. Create event is removed when it was present in previous event set.
// Example:
//
//	~ $ echo > file # Create|Write -> Create|Write
//	~ $ echo > file # Create|Write|InodeMetaMod -> Write|InodeMetaMod
//
// 2. Remove event is removed if it was present in previouse event set.
// Example:
//
//	~ $ touch file # Create -> Create
//	~ $ rm file    # Create|Remove -> Remove
//	~ $ touch file # Create|Remove -> Create
//
// 3. Write event is removed if not followed by InodeMetaMod on existing
// file. Example:
//
//	~ $ echo > file   # Create|Write -> Create|Write
//	~ $ chmod +x file # Create|Write|ChangeOwner -> ChangeOwner
//
// 4. Write&InodeMetaMod is removed when effective event set contain Remove event.
// Example:
//
//	~ $ echo > file # Write|InodeMetaMod -> Write|InodeMetaMod
//	~ $ rm file     # Remove|Write|InodeMetaMod -> Remove
func (w *watch) strip(base string, set uint32) uint32 {
	const (
		write = FSEventsModified | FSEventsInodeMetaMod
		both  = FSEventsCreated | FSEventsRemoved
	)
	prev := w.prev.get(base)
	next := prev
	switch prev {
	case FSEventsCreated:
		set &^= FSEventsCreated
		if set&FSEventsRemoved != 0 {
			next = FSEventsRemoved
			set &^= write
		}
	case FSEventsRemoved:
		set &^= FSEventsRemoved
		if set&FSEventsCreated != 0 {
			next = FSEventsCreated
		}
	default:
		switch set & both {
		case FSEventsCreated:
			next = FSEventsCreated
		case FSEventsRemoved:
			next = FSEventsRemoved
			set &^= write
		}
	}
	// Set the state even when it is unchanged, so that a path which keeps
	// changing stays in the current generation.
	if next != 0 {
		w.prev.set(base, next)
	}
	dbgprintf("split()=%v\n", Event(set))
	return set
}

// Dispatch is a stream function which forwards given file events for the watched
// path to underlying FileInfo channel.
func (w *watch) Dispatch(ev []FSEvent) {
	events := atomic.LoadUint32(&w.events)
	isrec := (atomic.LoadInt32(&w.isrec) == 1)
	for i := range ev {
		if ev[i].Flags&FSEventsHistoryDone != 0 {
			w.flushed = true
			continue
		}
		if !w.flushed {
			continue
		}
		dbgprintf("%v (0x%x) (%s, i=%d, ID=%d, len=%d)\n", Event(ev[i].Flags),
			ev[i].Flags, ev[i].Path, i, ev[i].ID, len(ev))
		if ev[i].Flags&failure != 0 && failure&events == 0 {
			// TODO(rjeczalik): missing error handling
			continue
		}
		if !strings.HasPrefix(ev[i].Path, w.path) {
			continue
		}
		n := len(w.path)
		base := ""
		if len(ev[i].Path) > n {
			if ev[i].Path[n] != '/' {
				continue
			}
			base = ev[i].Path[n+1:]
			if !isrec && strings.IndexByte(base, '/') != -1 {
				continue
			}
		}
		// TODO(rjeczalik): get diff only from filtered events?
		e := w.strip(base, ev[i].Flags) & events
		if e == 0 {
			continue
		}
		for _, e := range splitflags(e) {
			dbgprintf("%d: single event: %v", ev[i].ID, Event(e))
			w.c <- &event{
				fse:   ev[i],
				event: Event(e),
			}
		}
	}
}

// Stop closes underlying FSEvents stream and stops dispatching events.
func (w *watch) Stop() {
	w.stream.Stop()
	// TODO(rjeczalik): make (*stream).Stop flush synchronously undelivered events,
	// so the following hack can be removed. It should flush all the streams
	// concurrently as we care not to block too much here.
	atomic.StoreUint32(&w.events, 0)
	atomic.StoreInt32(&w.isrec, 0)
}

// fsevents implements Watcher and RecursiveWatcher interfaces backed by FSEvents
// framework.
type fsevents struct {
	watches map[string]*watch
	c       chan<- EventInfo
}

func newWatcher(c chan<- EventInfo) watcher {
	return &fsevents{
		watches: make(map[string]*watch),
		c:       c,
	}
}

func (fse *fsevents) watch(path string, event Event, isrec int32) (err error) {
	if _, ok := fse.watches[path]; ok {
		return errAlreadyWatched
	}
	w := &watch{
		c:      fse.c,
		path:   path,
		events: uint32(event),
		isrec:  isrec,
	}
	w.stream = newStream(path, w.Dispatch)
	if err = w.stream.Start(); err != nil {
		return err
	}
	fse.watches[path] = w
	return nil
}

func (fse *fsevents) unwatch(path string) (err error) {
	w, ok := fse.watches[path]
	if !ok {
		return errNotWatched
	}
	w.stream.Stop()
	delete(fse.watches, path)
	return nil
}

// Watch implements Watcher interface. It fails with non-nil error when setting
// the watch-point by FSEvents fails or with errAlreadyWatched error when
// the given path is already watched.
func (fse *fsevents) Watch(path string, event Event) error {
	return fse.watch(path, event, 0)
}

// Unwatch implements Watcher interface. It fails with errNotWatched when
// the given path is not being watched.
func (fse *fsevents) Unwatch(path string) error {
	return fse.unwatch(path)
}

// Rewatch implements Watcher interface. It fails with errNotWatched when
// the given path is not being watched or with errInvalidEventSet when oldevent
// does not match event set the watch-point currently holds.
func (fse *fsevents) Rewatch(path string, oldevent, newevent Event) error {
	w, ok := fse.watches[path]
	if !ok {
		return errNotWatched
	}
	if !atomic.CompareAndSwapUint32(&w.events, uint32(oldevent), uint32(newevent)) {
		return errInvalidEventSet
	}
	atomic.StoreInt32(&w.isrec, 0)
	return nil
}

// RecursiveWatch implements RecursiveWatcher interface. It fails with non-nil
// error when setting the watch-point by FSEvents fails or with errAlreadyWatched
// error when the given path is already watched.
func (fse *fsevents) RecursiveWatch(path string, event Event) error {
	return fse.watch(path, event, 1)
}

// RecursiveUnwatch implements RecursiveWatcher interface. It fails with
// errNotWatched when the given path is not being watched.
//
// TODO(rjeczalik): fail if w.isrec == 0?
func (fse *fsevents) RecursiveUnwatch(path string) error {
	return fse.unwatch(path)
}

// RecursiveRewatch implements RecursiveWatcher interface. It fails:
//
//   - with errNotWatched when the given path is not being watched
//   - with errInvalidEventSet when oldevent does not match the current event set
//   - with errAlreadyWatched when watch-point given by the oldpath was meant to
//     be relocated to newpath, but the newpath is already watched
//   - a non-nil error when setting the watch-point with FSEvents fails
//
// TODO(rjeczalik): Improve handling of watch-point relocation? See two TODOs
// that follows.
func (fse *fsevents) RecursiveRewatch(oldpath, newpath string, oldevent, newevent Event) error {
	switch [2]bool{oldpath == newpath, oldevent == newevent} {
	case [2]bool{true, true}:
		w, ok := fse.watches[oldpath]
		if !ok {
			return errNotWatched
		}
		atomic.StoreInt32(&w.isrec, 1)
		return nil
	case [2]bool{true, false}:
		w, ok := fse.watches[oldpath]
		if !ok {
			return errNotWatched
		}
		if !atomic.CompareAndSwapUint32(&w.events, uint32(oldevent), uint32(newevent)) {
			return errors.New("invalid event state diff")
		}
		atomic.StoreInt32(&w.isrec, 1)
		return nil
	default:
		// TODO(rjeczalik): rewatch newpath only if exists?
		// TODO(rjeczalik): migrate w.prev to new watch?
		if _, ok := fse.watches[newpath]; ok {
			return errAlreadyWatched
		}
		if err := fse.Unwatch(oldpath); err != nil {
			return err
		}
		// TODO(rjeczalik): revert unwatch if watch fails?
		return fse.watch(newpath, newevent, 1)
	}
}

// Close unwatches all watch-points.
func (fse *fsevents) Close() error {
	for _, w := range fse.watches {
		w.Stop()
	}
	fse.watches = nil
	return nil
}
