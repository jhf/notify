// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package notify

import (
	"sync/atomic"
	"time"
)

// PollTickName is the name of the synthetic event emitted when a poll-mode
// replay completes. It passes the consumer's watch filter via the barrier
// canary exception (prefix .syncthing.barrier-, suffix .tmp) so its event ID
// reaches the watermark batcher even when the gap contained no file events;
// the scanner ignores it as a temporary file, so it is never indexed.
const PollTickName = ".syncthing.barrier-poll.tmp"

// pollOnlyNanos is the poll-mode interval; 0 disables poll mode (default:
// the classic live stream).
var pollOnlyNanos atomic.Int64

// SetPollOnly switches FSEvents watching to poll mode: instead of keeping a
// live stream running (whose per-event kernel journaling load grows with
// churn until fseventsd overflows and the consumer heals in a vicious
// cycle), each watch replays the journal from its watermark, stops the
// stream at kFSEventStreamEventIdHistoryDone, and re-arms a one-shot replay
// after d. Falling behind is graceful: the journal is a queue, so the gap
// simply grows; there are no drops, no heals, no stream restarts. Boot and
// poll paths are identical, so every recovery is one replay.
func SetPollOnly(d time.Duration) {
	pollOnlyNanos.Store(int64(d))
}

// PollInterval reports the poll-mode interval, 0 when disabled.
func PollInterval() time.Duration {
	return time.Duration(pollOnlyNanos.Load())
}

// pollHistoryDone replaces the live-stream HistoryDone handling in poll
// mode: persist the high-water mark, tick the consumer, stop the stream,
// and schedule the next poll. If a drop flag was deferred during this
// poll's replay, the next poll starts from the hole start rather than the
// end (same semantics as the live-mode follow-up replay).
func (w *watch) pollHistoryDone(id uint64, d time.Duration) {
	w.lastID.Store(id)
	from := id
	if w.hasPending {
		from = w.pendingFrom
		healprintf("poll hole deferred path=%s from=%d target=%d count=%d\n",
			w.path, w.pendingFrom, w.pendingTarget, w.pendingCount)
		w.hasPending = false
		w.pendingCount = 0
	}
	select {
	case w.c <- &event{fse: FSEvent{Path: w.path + "/" + PollTickName, ID: id, Flags: uint32(FSEventsCreated | FSEventsIsFile)}, event: Create}:
	default:
	}
	if w.stream != nil {
		w.stream.Stop()
	}
	w.live = false
	healprintf("poll done path=%s ID=%d next from=%d in=%v\n", w.path, id, from, d)
	if w.scheduleRestart != nil {
		w.scheduleRestart(from, d)
	}
}
