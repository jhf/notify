// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package notify

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestSplitflags(t *testing.T) {
	cases := [...]struct {
		set   uint32
		flags []uint32
	}{
		{0, nil},
		{0xD, []uint32{0x1, 0x4, 0x8}},
		{0x0010 | 0x0040 | 0x0080 | 0x01000, []uint32{0x0010, 0x0040, 0x0080, 0x01000}},
		{0x40000 | 0x00100 | 0x00200, []uint32{0x00100, 0x00200, 0x40000}},
	}
	for i, cas := range cases {
		if flags := splitflags(cas.set); !reflect.DeepEqual(flags, cas.flags) {
			t.Errorf("want flags=%v; got %v (i=%d)", cas.flags, flags, i)
		}
	}
}

func TestWatchStrip(t *testing.T) {
	const (
		create = uint32(FSEventsCreated)
		remove = uint32(FSEventsRemoved)
		rename = uint32(FSEventsRenamed)
		write  = uint32(FSEventsModified)
		inode  = uint32(FSEventsInodeMetaMod)
		owner  = uint32(FSEventsChangeOwner)
	)
	cases := [...][]struct {
		path string
		flag uint32
		diff uint32
	}{
		// 1.
		{
			{"file", create | write, create | write},
			{"file", create | write | inode, write | inode},
		},
		// 2.
		{
			{"file", create, create},
			{"file", create | remove, remove},
			{"file", create | remove, create},
		},
		// 3.
		{
			{"file", create | write, create | write},
			{"file", create | write | owner, write | owner},
		},
		// 4.
		{
			{"file", create | write, create | write},
			{"file", write | inode, write | inode},
			{"file", remove | write | inode, remove},
		},
		{
			{"file", remove | write | inode, remove},
		},
	}
Test:
	for i, cas := range cases {
		if len(cas) == 0 {
			t.Log("skipped")
			continue
		}
		w := &watch{}
		for j, cas := range cas {
			if diff := w.strip(cas.path, cas.flag); diff != cas.diff {
				t.Errorf("want diff=%v; got %v (i=%d, j=%d)", Event(cas.diff),
					Event(diff), i, j)
				continue Test
			}
		}
	}
}

// fillHistory records the creation of n other paths.
func fillHistory(w *watch, prefix string, n int) {
	for i := 0; i < n; i++ {
		w.strip(fmt.Sprintf("%s/%d", prefix, i), uint32(FSEventsCreated))
	}
}

func TestWatchStripHistory(t *testing.T) {
	const (
		create = uint32(FSEventsCreated)
		remove = uint32(FSEventsRemoved)
		write  = uint32(FSEventsModified)
	)
	cases := [...]struct {
		name       string
		setup      []uint32
		next       uint32
		remembered uint32
		forgotten  uint32
	}{
		{"created", []uint32{create}, create | write, write, create | write},
		{"removed", []uint32{create, create | remove}, create | remove, create, create | remove},
	}
	for _, cas := range cases {
		// Where the path lands in its generation must not matter.
		for _, offset := range []int{0, 1, historyLen / 2, historyLen - 1} {
			for _, other := range []struct {
				n    int
				want uint32
			}{
				{historyLen, cas.remembered},
				{2 * historyLen, cas.forgotten},
			} {
				w := &watch{}
				fillHistory(w, "before", offset)
				for _, flags := range cas.setup {
					w.strip("file", flags)
				}
				fillHistory(w, "after", other.n)
				if diff := w.strip("file", cas.next); diff != other.want {
					t.Errorf("%s, offset %d, %d other paths: want diff=%v; got %v",
						cas.name, offset, other.n, Event(other.want), Event(diff))
				}
			}
		}
	}

	// A path that keeps changing must never be forgotten.
	w := &watch{}
	w.strip("file", create)
	for i := 0; i < 8; i++ {
		fillHistory(w, fmt.Sprintf("round%d", i), historyLen/2+1)
		if diff := w.strip("file", create|write); diff != write {
			t.Fatalf("round %d: want diff=%v; got %v", i, Event(write), Event(diff))
		}
	}
}

func TestWatchDispatchBoundedHistory(t *testing.T) {
	// The channel must be large enough to hold every event: Dispatch no
	// longer blocks on a full channel (it drops and emits a synthetic
	// MustScanSubDirs instead), so a small buffer would lose events here.
	// Full delivery when the consumer keeps up is part of what this test
	// pins down, alongside history bounding.
	c := make(chan EventInfo, 8*historyLen+1024)
	got := make(chan map[Event]int)
	go func() {
		count := make(map[Event]int)
		for ei := range c {
			count[ei.Event()]++
		}
		got <- count
	}()
	w := &watch{
		c:      c,
		path:   "/watched",
		events: uint32(Create | Remove | Rename),
		isrec:  1,
	}
	w.Dispatch([]FSEvent{{Path: "/watched", Flags: uint32(FSEventsHistoryDone)}})
	const n = 4 * historyLen
	for i := 0; i < n; i++ {
		// A temporary file that is removed, and one that is renamed into
		// place elsewhere, as build tools and downloaders do.
		tmp := fmt.Sprintf("/watched/build/%d.o", i)
		part := fmt.Sprintf("/watched/downloads/%d.part", i)
		w.Dispatch([]FSEvent{
			{Path: tmp, Flags: uint32(FSEventsCreated | FSEventsIsFile)},
			{Path: part, Flags: uint32(FSEventsCreated | FSEventsIsFile)},
			{Path: tmp, Flags: uint32(FSEventsCreated | FSEventsRemoved | FSEventsIsFile)},
			{Path: part, Flags: uint32(FSEventsCreated | FSEventsRenamed | FSEventsIsFile)},
		})
		if size := len(w.prev.cur) + len(w.prev.old); size > 2*historyLen {
			t.Fatalf("history holds %d paths after %d files; want at most %d",
				size, 2*(i+1), 2*historyLen)
		}
	}
	close(c)
	want := map[Event]int{Create: 2 * n, Remove: n, Rename: n}
	if count := <-got; !reflect.DeepEqual(count, want) {
		t.Errorf("want events %v; got %v", want, count)
	}
}

// Test for cases 3) and 5) with shadowed write&create events.
//
// See comment for (flagdiff).diff method.
func TestWatcherShadowedWriteCreate(t *testing.T) {
	w := NewWatcherTest(t, "testdata/vfs.txt")
	defer w.Close()

	cases := [...]WCase{
		// i=0
		create(w, "src/github.com/rjeczalik/fs/.fs.go.swp"),
		// i=1
		write(w, "src/github.com/rjeczalik/fs/.fs.go.swp", []byte("XD")),
		// i=2
		write(w, "src/github.com/rjeczalik/fs/.fs.go.swp", []byte("XD")),
		// i=3
		remove(w, "src/github.com/rjeczalik/fs/.fs.go.swp"),
		// i=4
		create(w, "src/github.com/rjeczalik/fs/.fs.go.swp"),
		// i=5
		write(w, "src/github.com/rjeczalik/fs/.fs.go.swp", []byte("XD")),
	}

	w.ExpectAny(cases[:5]) // BUG(rjeczalik): #62
}

func TestDispatchDropsWithSyntheticFlag(t *testing.T) {
	// A full consumer channel must not block Dispatch (it runs on the serial
	// fseventsd delivery queue); dropped events must surface as one synthetic
	// MustScanSubDirs event for the watched root.
	c := make(chan EventInfo, 4)
	w := &watch{
		c:       c,
		path:    "/watched",
		events:  uint32(FSEventsCreated | FSEventsModified),
		isrec:   1,
		flushed: true,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// 8 fresh paths, one Created send each, into a 4-buffer with no
		// consumer: must not block.
		for i := 0; i < 8; i++ {
			w.Dispatch([]FSEvent{{Path: fmt.Sprintf("/watched/f%d", i), Flags: FSEventsCreated}})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Dispatch blocked on a full consumer channel")
	}
	if w.dropped.Load() == 0 {
		t.Fatal("expected dropped event count > 0")
	}
	// Drain two slots; the next Dispatch must emit the synthetic flag,
	// followed by its own event.
	<-c
	<-c
	w.Dispatch([]FSEvent{{Path: "/watched/g", Flags: FSEventsCreated}})
	<-c // f2
	<-c // f3
	ei := <-c
	if ei.Event()&FSEventsMustScanSubDirs == 0 {
		t.Fatalf("want synthetic MustScanSubDirs after drops, got %v", ei.Event())
	}
	if ei.Path() != "/watched" {
		t.Fatalf("want synthetic event on watch root, got %q", ei.Path())
	}
	if ei := <-c; ei.Event() != Create || ei.Path() != "/watched/g" {
		t.Fatalf("want Create /watched/g after synthetic, got %v %q", ei.Event(), ei.Path())
	}
	if w.dropped.Load() != 0 {
		t.Fatalf("dropped count not reset: %d", w.dropped.Load())
	}
}

func TestDispatchHealsUserDroppedByReplay(t *testing.T) {
	// A live UserDropped flag must restart the stream replaying from the
	// last examined event ID, and must not be forwarded (the consumer must
	// not full-scan). Drop flags are delivery-time annotations, so a flagged
	// record arriving via the heal replay marks a FRESH hole in that
	// delivery: it is stripped and collected, and HistoryDone starts one
	// follow-up replay from the hole start.
	replayHistorical.Store(true)
	defer replayHistorical.Store(false)

	c := make(chan EventInfo, 64)
	var heals int
	var healFrom uint64
	w := &watch{
		c:       c,
		path:    "/watched",
		events:  uint32(Create | Remove | Rename | FSEventsMustScanSubDirs | FSEventsUserDropped | FSEventsKernelDropped),
		isrec:   1,
		flushed: true,
		live:    true,
	}
	w.restartStream = func(sinceID uint64) error {
		heals++
		healFrom = sinceID
		return nil
	}
	ud := uint32(FSEventsUserDropped | FSEventsMustScanSubDirs)

	// Normal live event: forwarded, lastID advances.
	w.Dispatch([]FSEvent{{Path: "/watched/a", Flags: uint32(FSEventsCreated | FSEventsIsFile), ID: 100}})
	// Live flagged event: heal from 100, nothing forwarded for it.
	w.Dispatch([]FSEvent{{Path: "/watched/b", Flags: ud | uint32(FSEventsCreated|FSEventsIsFile), ID: 105}})
	if heals != 1 || healFrom != 100 {
		t.Fatalf("want one heal from ID 100, got heals=%d from=%d", heals, healFrom)
	}
	if w.live {
		t.Fatal("watch still live after heal")
	}
	// Heal replay: a plain historical event, then the flagged record again -
	// a FRESH hole in this delivery, stripped and deferred - then
	// HistoryDone, which starts the follow-up heal from 101.
	w.Dispatch([]FSEvent{
		{Path: "/watched/c", Flags: uint32(FSEventsCreated | FSEventsIsFile), ID: 101},
		{Path: "/watched/b", Flags: ud | uint32(FSEventsCreated|FSEventsIsFile), ID: 105},
		{Flags: uint32(FSEventsHistoryDone), ID: 106},
	})
	if heals != 2 || healFrom != 101 {
		t.Fatalf("want follow-up heal from ID 101, got heals=%d from=%d", heals, healFrom)
	}
	if w.live {
		t.Fatal("watch must not be live during the follow-up replay")
	}
	// The follow-up replay is clean; HistoryDone restores live.
	w.Dispatch([]FSEvent{
		{Path: "/watched/b", Flags: uint32(FSEventsCreated | FSEventsIsFile), ID: 105},
		{Flags: uint32(FSEventsHistoryDone), ID: 106},
	})
	if !w.live {
		t.Fatal("watch not live after clean follow-up HistoryDone")
	}
	// A second live drop heals again, from the last examined ID.
	w.Dispatch([]FSEvent{{Path: "/watched/d", Flags: ud, ID: 200}})
	if heals != 3 || healFrom != 105 {
		t.Fatalf("want third heal from ID 105, got heals=%d from=%d", heals, healFrom)
	}

	close(c)
	var got []string
	for ei := range c {
		got = append(got, ei.Event().String()+" "+ei.Path())
	}
	// The clean re-delivery of /watched/b in the follow-up replay is
	// suppressed by strip's create-after-create dedup, so b appears once.
	want := []string{
		"notify.Create /watched/a",
		"notify.Create /watched/c",
		"notify.Create /watched/b",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("forwarded events:\n got %q\nwant %q", got, want)
	}
}

func TestDispatchForwardsBootReplayFlags(t *testing.T) {
	// Conservative boot path: a flagged event arriving as the FIRST record a
	// stream examines (nothing delivered yet, so the journal may genuinely
	// not cover the persisted seed) is forwarded unchanged and does not
	// trigger a heal - only a consumer full-scan covers a gap below the
	// replay start.
	replayHistorical.Store(true)
	defer replayHistorical.Store(false)

	c := make(chan EventInfo, 64)
	w := &watch{
		c:       c,
		path:    "/watched",
		events:  uint32(Create | FSEventsMustScanSubDirs | FSEventsUserDropped | FSEventsKernelDropped),
		isrec:   1,
		flushed: true,
		live:    false,
	}
	w.restartStream = func(uint64) error {
		t.Error("heal must not trigger before the stream is live")
		return nil
	}
	ud := uint32(FSEventsUserDropped | FSEventsMustScanSubDirs)
	w.Dispatch([]FSEvent{{Path: "/watched/x", Flags: ud, ID: 50}})
	close(c)
	var got []Event
	for ei := range c {
		got = append(got, ei.Event())
	}
	if len(got) == 0 {
		t.Fatal("boot replay flag was swallowed; consumer would never full-scan")
	}
	for _, e := range got {
		if e&(FSEventsMustScanSubDirs|FSEventsUserDropped) == 0 {
			t.Fatalf("unexpected event forwarded: %v", e)
		}
	}
}

func TestDispatchHealsMidReplayFlag(t *testing.T) {
	// A flagged record arriving mid-replay (the stream has already examined
	// records, so the journal provably covers the replay start) marks a hole
	// INSIDE the replayed range. It must be stripped and collected for ONE
	// deferred follow-up heal at HistoryDone - restarting per flag stormed
	// (measured 1757 restarts in 10 minutes without converging), and
	// forwarding each flag fed the consumer's full-scan loop.
	replayHistorical.Store(true)
	defer replayHistorical.Store(false)

	c := make(chan EventInfo, 64)
	var heals int
	var healFrom, healTarget uint64
	w := &watch{
		c:         c,
		path:      "/watched",
		events:    uint32(Create | FSEventsMustScanSubDirs | FSEventsUserDropped | FSEventsKernelDropped),
		isrec:     1,
		flushed:   true,
		live:      false, // mid boot replay: no HistoryDone yet
		replaying: true,  // dev.85 gate: replay-time flags defer
	}
	w.restartStream = func(sinceID uint64) error {
		heals++
		healFrom = sinceID
		healTarget = w.healTargetID.Load()
		return nil
	}
	ud := uint32(FSEventsUserDropped | FSEventsMustScanSubDirs)

	// First replayed record is clean: the journal covers the start.
	w.Dispatch([]FSEvent{{Path: "/watched/a", Flags: uint32(FSEventsCreated | FSEventsIsFile), ID: 10}})
	// Two mid-replay flagged records: stripped, collected, no heal yet.
	w.Dispatch([]FSEvent{
		{Path: "/watched/b", Flags: ud, ID: 20},
		{Path: "/watched/d", Flags: ud, ID: 30},
	})
	if heals != 0 {
		t.Fatalf("mid-replay flags must defer, heals=%d", heals)
	}
	// HistoryDone starts the single follow-up heal from the first hole (10,
	// the last examined ID before the first flag) covering up to 30.
	w.Dispatch([]FSEvent{{Flags: uint32(FSEventsHistoryDone), ID: 31}})
	if heals != 1 || healFrom != 10 || healTarget != 30 {
		t.Fatalf("want one follow-up heal from=10 target=30, got heals=%d from=%d target=%d", heals, healFrom, healTarget)
	}
	if w.live {
		t.Fatal("watch must not be live during the follow-up replay")
	}
	// The follow-up replay re-delivers the range. Flags are delivery-time
	// annotations, so a CLEAN re-delivery carries no flags; HistoryDone
	// (nothing pending) goes live.
	w.Dispatch([]FSEvent{
		{Path: "/watched/c", Flags: uint32(FSEventsCreated | FSEventsIsFile), ID: 15},
		{Path: "/watched/b", Flags: uint32(FSEventsCreated | FSEventsIsFile), ID: 20},
		{Flags: uint32(FSEventsHistoryDone), ID: 31},
	})
	if !w.live {
		t.Fatal("watch not live after clean follow-up HistoryDone")
	}
	if heals != 1 {
		t.Fatalf("clean re-delivery must not re-heal, heals=%d", heals)
	}

	close(c)
	var got []string
	for ei := range c {
		got = append(got, ei.Event().String()+" "+ei.Path())
	}
	want := []string{
		"notify.Create /watched/a",
		"notify.Create /watched/c",
		"notify.Create /watched/b",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("forwarded events:\n got %q\nwant %q", got, want)
	}
}

func TestDispatchLossyReplayForwardsOneRootRescan(t *testing.T) {
	// A replay that collects maxSurgicalHoles or more drop holes is too
	// lossy for surgical follow-ups: HistoryDone forwards ONE root rescan
	// (consumer full-scan) instead of restarting the stream.
	replayHistorical.Store(true)
	defer replayHistorical.Store(false)

	c := make(chan EventInfo, 2*maxSurgicalHoles)
	var heals int
	w := &watch{
		c:         c,
		path:      "/watched",
		events:    uint32(Create | FSEventsMustScanSubDirs | FSEventsUserDropped | FSEventsKernelDropped),
		isrec:     1,
		flushed:   true,
		replaying: true,
	}
	w.restartStream = func(uint64) error { heals++; return nil }
	ud := uint32(FSEventsUserDropped | FSEventsMustScanSubDirs)

	w.Dispatch([]FSEvent{{Path: "/watched/a", Flags: uint32(FSEventsCreated | FSEventsIsFile), ID: 1}})
	batch := make([]FSEvent, 0, maxSurgicalHoles+1)
	for i := 0; i < maxSurgicalHoles; i++ {
		batch = append(batch, FSEvent{Path: "/watched/x", Flags: ud, ID: uint64(10 + i)})
	}
	batch = append(batch, FSEvent{Flags: uint32(FSEventsHistoryDone), ID: 100})
	w.Dispatch(batch)

	if heals != 0 {
		t.Fatalf("lossy replay must not trigger a surgical follow-up, heals=%d", heals)
	}
	var rootRescans int
	close(c)
	for ei := range c {
		if ei.Event()&FSEventsMustScanSubDirs != 0 {
			rootRescans++
		}
	}
	if rootRescans != 1 {
		t.Fatalf("want exactly one root rescan forwarded, got %d", rootRescans)
	}
	if !w.flushed {
		t.Fatal("HistoryDone must still mark the stream flushed")
	}
	if !w.live {
		t.Fatal("lossy verdict must restore live: HistoryDone means the stream " +
			"has caught up; staying live=false defers every later drop flag " +
			"into a pending set no future HistoryDone ever judges")
	}

	// After the lossy verdict the stream is live again: the next drop flag
	// must take the immediate per-flag heal path, not the deferral path.
	c2 := make(chan EventInfo, 4)
	w.c = c2
	w.Dispatch([]FSEvent{{Path: "/watched/y", Flags: ud, ID: 200}})
	if heals != 1 {
		t.Fatalf("post-verdict drop flag must heal immediately, heals=%d", heals)
	}
	if w.live {
		t.Fatal("watch must not be live during the new heal replay")
	}
	if w.hasPending {
		t.Fatal("post-verdict drop flag must not be deferred")
	}
	close(c2)
}
