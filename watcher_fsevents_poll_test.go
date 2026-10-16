// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package notify

import (
	"strings"
	"testing"
	"time"
)

func TestPollModeHistoryDoneStopsAndReschedules(t *testing.T) {
	// Poll mode: HistoryDone stops the stream and schedules the next
	// one-shot replay instead of marking the watch live. The consumer gets
	// a tick event carrying the high-water ID so it can persist its
	// watermark even when the gap delivered no file events.
	SetPollOnly(5 * time.Second)
	defer SetPollOnly(0)
	replayHistorical.Store(true)
	defer replayHistorical.Store(false)

	c := make(chan EventInfo, 64)
	var restarts int
	var restartFrom uint64
	var restartDelay time.Duration
	w := &watch{
		c:         c,
		path:      "/watched",
		events:    uint32(Create | FSEventsMustScanSubDirs | FSEventsUserDropped | FSEventsKernelDropped),
		isrec:     1,
		flushed:   true,
		replaying: true,
	}
	w.restartStream = func(uint64) error { t.Fatal("live restartStream must not run in poll mode"); return nil }
	w.scheduleRestart = func(from uint64, d time.Duration) {
		restarts++
		restartFrom = from
		restartDelay = d
	}

	w.Dispatch([]FSEvent{{Path: "/watched/a", Flags: uint32(FSEventsCreated | FSEventsIsFile), ID: 100}})
	w.Dispatch([]FSEvent{{Flags: uint32(FSEventsHistoryDone), ID: 101}})

	if restarts != 1 || restartFrom != 101 || restartDelay != 5*time.Second {
		t.Fatalf("want one scheduled restart from=101 in=5s, got n=%d from=%d in=%v",
			restarts, restartFrom, restartDelay)
	}
	if w.live {
		t.Fatal("poll mode must not mark the watch live at HistoryDone")
	}
	if w.replaying {
		t.Fatal("HistoryDone must clear replaying")
	}
	if got := w.lastID.Load(); got != 101 {
		t.Fatalf("lastID = %d, want 101", got)
	}
	// Expect the file event, then the tick carrying ID 101.
	close(c)
	var events []EventInfo
	for ei := range c {
		events = append(events, ei)
	}
	if len(events) != 2 {
		t.Fatalf("want file event + tick, got %d events", len(events))
	}
	tick := events[1]
	if !strings.HasSuffix(tick.Path(), "/"+PollTickName) {
		t.Fatalf("tick path = %q, want suffix %q", tick.Path(), PollTickName)
	}
	if id := tick.Sys().(interface{ EventID() uint64 }).EventID(); id != 101 {
		t.Fatalf("tick ID = %d, want 101 (watermark high-water mark)", id)
	}
}

func TestPollModeDisabledKeepsLiveStream(t *testing.T) {
	// Teeth for the poll branch: with poll mode off, HistoryDone keeps the
	// classic live-stream semantics and nothing is scheduled.
	replayHistorical.Store(true)
	defer replayHistorical.Store(false)

	c := make(chan EventInfo, 64)
	w := &watch{
		c:       c,
		path:    "/watched",
		events:  uint32(Create | FSEventsMustScanSubDirs | FSEventsUserDropped | FSEventsKernelDropped),
		isrec:   1,
		flushed: true,
	}
	w.scheduleRestart = func(uint64, time.Duration) {
		t.Fatal("scheduleRestart must not run with poll mode disabled")
	}
	w.Dispatch([]FSEvent{{Flags: uint32(FSEventsHistoryDone), ID: 50}})
	if !w.live {
		t.Fatal("HistoryDone must mark the watch live when poll mode is disabled")
	}
}

func TestPollModeDefersHoleToNextPoll(t *testing.T) {
	// A drop flag during a poll's replay marks a delivery hole; the next
	// poll must restart from the hole start, not the replay end - same
	// semantics as the live-mode deferred follow-up heal.
	SetPollOnly(2 * time.Second)
	defer SetPollOnly(0)
	replayHistorical.Store(true)
	defer replayHistorical.Store(false)

	c := make(chan EventInfo, 64)
	var restartFrom uint64
	w := &watch{
		c:         c,
		path:      "/watched",
		events:    uint32(Create | FSEventsMustScanSubDirs | FSEventsUserDropped | FSEventsKernelDropped),
		isrec:     1,
		flushed:   true,
		replaying: true,
	}
	w.scheduleRestart = func(from uint64, d time.Duration) { restartFrom = from }

	ud := uint32(FSEventsUserDropped | FSEventsMustScanSubDirs)
	w.Dispatch([]FSEvent{{Path: "/watched/a", Flags: uint32(FSEventsCreated | FSEventsIsFile), ID: 10}})
	w.Dispatch([]FSEvent{{Path: "/watched/b", Flags: ud, ID: 20}})
	w.Dispatch([]FSEvent{{Flags: uint32(FSEventsHistoryDone), ID: 30}})

	if restartFrom != 10 {
		t.Fatalf("next poll must start from the hole start 10, got %d", restartFrom)
	}
	if w.hasPending {
		t.Fatal("pending hole must be consumed by the scheduling")
	}
}

// The fork's 1s stream latency (watcher_fsevents_cgo.go) breaks the phase
// isolation the shared test cases rely on: with coalescing, an action's
// events can land in the NEXT phase's expectation window (observed
// 2026-10-16: TestWatcherRecursiveRewatch got a straggler dir-create from
// its first block while expecting the second block's file-create). Tests
// assert delivery semantics, not coalescing, so run them at the upstream
// latency.
func init() {
	latency = 0
}
