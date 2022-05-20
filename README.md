# hoard

A tool to allow data hoarders to split data sets across a pool of offline external hard drives.

> I want to manage 100TB of static data sets across my shelf full of hard drives in a way that won't enrage me.

## Basic Usage

```bash
$ hoard disk add --label russia-leaks-01 /dev/sdb
$ hoard partition add /dev/sdb1
$ hoard collection add vgtrk
$ hoard file add -c vgtrk ~/torrents/some-leak/path-to-file.zip /some-leak/path-to-file.zip
$ umount /dev/sdb1  # can still search files while devices not mounted
$ hoard file ls -c vgtrk /some-leak/
```

## Requirements

This currently only supports Linux.

```bash
sudo apt install -y sqlite3 libudev-dev
```

## License

`hoard` is licensed under a MIT license.
The full text can be found under [`LICENSE`](./LICENSE).
