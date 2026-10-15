# Maintained patches on top of upstream notify

This fork of [syncthing/notify](https://github.com/syncthing/notify) carries
the FSEvents journal-replay series used by the
[jhf/syncthing](https://github.com/jhf/syncthing) soft fork (see its
PATCHES.md and [syncthing#10921](https://github.com/syncthing/syncthing/issues/10921)).

All patches live on the `fsevents-replay` branch (the development branch);
`main` is that branch plus this documentation. The series is one cohesive
feature and is kept as a single branch; the pieces in order:

| Commit | Piece |
|--------|-------|
| `fbdebde` | Bound the FSEvents watcher's path history (merged from upstream work) |
| `272bca3` | Journal replay primitives: `SetSinceEventID` / `ResetSinceEventID` / `CurrentEventID`; a consumer can persist a high-water mark and replay the journal from it instead of rescanning |
| `79d5886` | Never block the delivery queue; drop with a synthetic rescan flag |
| `37a442f` | Heal UserDropped by journal replay instead of making the consumer full-scan |
| `0f37af6` | NOTIFY_DEBUG_HEAL logging for heal decisions (env-gated instrumentation; drop before upstreaming) |
| `c473a2f` | Defer mid-replay drop heals to one follow-up at HistoryDone |
| `be89a9e` | Treat every mid-replay flag as a fresh hole; lossy threshold |
| `df43a3b` | Restore live state on the lossy-replay verdict |
| `1f2f7bb` | ReplayEnabled reports whether drop flags will be healed |
| `3b2657e` | Defer drop flags during the whole replay, not just after first record |
| `9a7d1df` | 1 s stream latency instead of 0 (batches events; far fewer wakeups) |
| `6de31c6` | IgnoreSelf: filter our own process's events server-side via FSEvents device-independent filtering |
| `2721ae2` | Poll-only mode (`SetPollOnly`): replay the journal periodically instead of holding a live stream; the watcher becomes an occasional batch reader |

## Headline results (in syncthing, on this fork, Oct 2026)

- Boot replay of a 4.17M-event / 5.8-hour FSEvents journal gap: 6 s, no scan.
- Poll mode at 5 s cadence: fseventsd 0.2–3 % CPU / 14–16 MB RSS (was
  104 % / 19.3 GB under event storms with a live stream); zero drop flags
  across 11,877+ poll cycles on a 2.4M-file tree.

## Maintenance

Same model as the syncthing fork: track upstream `main`, rebase this series
per upstream movement, keep the pieces small and in order. If upstream
engages, split into one PR per piece.
