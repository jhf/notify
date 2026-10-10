// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package notify

import (
	"time"
	"fmt"
	"reflect"
	"testing"
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
