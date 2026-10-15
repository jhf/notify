> **This is a maintained soft fork of [syncthing/notify](https://github.com/syncthing/notify).**
> It carries the FSEvents journal-replay series (replay instead of rescan,
> IgnoreSelf, poll-only mode) listed in [PATCHES.md](PATCHES.md), used by
> [jhf/syncthing](https://github.com/jhf/syncthing). For everything else,
> refer to the upstream project below.

---

notify [![GoDoc](https://godoc.org/github.com/rjeczalik/notify?status.svg)](https://godoc.org/github.com/rjeczalik/notify) [![Build Status](https://img.shields.io/travis/rjeczalik/notify/master.svg)](https://travis-ci.org/rjeczalik/notify "inotify + FSEvents + kqueue") [![Build status](https://img.shields.io/appveyor/ci/rjeczalik/notify-246.svg)](https://ci.appveyor.com/project/rjeczalik/notify-246 "ReadDirectoryChangesW") [![Coverage Status](https://img.shields.io/coveralls/rjeczalik/notify/master.svg)](https://coveralls.io/r/rjeczalik/notify?branch=master)
======

Filesystem event notification library on steroids.

*Documentation*

[godoc.org/github.com/rjeczalik/notify](https://godoc.org/github.com/rjeczalik/notify)

*Installation*

```
~ $ go get -u github.com/rjeczalik/notify
```

*Projects using notify*

- [github.com/rjeczalik/cmd/notify](https://godoc.org/github.com/rjeczalik/cmd/notify)
- [github.com/cortesi/devd](https://github.com/cortesi/devd)
- [github.com/cortesi/modd](https://github.com/cortesi/modd)
- [github.com/syncthing/syncthing](https://github.com/syncthing/syncthing)
- [github.com/OrlovEvgeny/TinyJPG](https://github.com/OrlovEvgeny/TinyJPG)
- [github.com/mitranim/gow](https://github.com/mitranim/gow)
