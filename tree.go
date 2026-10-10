// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

package notify

// buffer is the capacity of the channel between the OS watcher callback and
// the tree's dispatch loop. On Darwin the callback runs on a serial dispatch
// queue shared with fseventsd delivery: if this buffer fills, the queue
// stalls and fseventsd starts dropping events for the stream, flagging later
// events with MustScanSubDirs/UserDropped. 128 entries is only ~280ms of
// headroom at a 460 events/s replay flood (observed stalling a busy machine,
// 2026-10-10); 65536 (1 MB of interface words) absorbs multi-minute floods.
const buffer = 65536

type tree interface {
	Watch(string, chan<- EventInfo, DoNotWatchFn, ...Event) error
	Stop(chan<- EventInfo)
	Close() error
}

func newTree() tree {
	c := make(chan EventInfo, buffer)
	w := newWatcher(c)
	if rw, ok := w.(recursiveWatcher); ok {
		return newRecursiveTree(rw, c)
	}
	return newNonrecursiveTree(w, c, make(chan EventInfo, buffer))
}
