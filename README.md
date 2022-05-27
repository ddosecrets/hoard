# hoard

A tool to allow data hoarders to split data sets across a pool of offline external hard drives.

> I want to manage 100TB of static data sets across my shelf full of hard drives in a way that won't enrage me.

**WARNING: alpha software**

## Basic Usage

```bash
hoard init
hoard disk add --label russia-leaks-01 /dev/sdb
hoard partition add /dev/sdb1
hoard collection add vgtrk
hoard file add -c vgtrk ~/torrents/some-leak/path-to-file.zip /some-leak/path-to-file.zip
umount /dev/sdb1  # can still search files while devices not mounted
hoard file ls -c vgtrk /some-leak/
```

## Building

This currently only supports Linux.

```bash
sudo apt install -y \
    libudev-dev \
    liblzma-dev
```

You will need Rust and `cargo`, which can be gotten with [`rustup`](https://rustup.rs/).
You can build the release binary like so:

```bash
cargo build --release
mv target/release/hoard ~/.local/bin/  # assumes ~/.local/bin is on your $PATH
hoard --help
```

## Docs

Better docs can be found in the [`cli` module](./src/cli.rs) or by running `cargo doc --open`.

## License

`hoard` is licensed under a MIT license.
The full text can be found under [`LICENSE`](./LICENSE).
