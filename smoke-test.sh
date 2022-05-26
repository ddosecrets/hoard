#!/usr/bin/env bash

# helper script to run through all the major command of `hoard`
# as a sort of integration test

set -eu

declare -ra hoard=(
    ./target/debug/hoard
    -vv
    --no-migrate
)

declare -r SQLITE_DB='./db.sqlite'
declare -r COLLECTION='some-collection'
declare -r LOCAL_FILE='/tmp/a5b63c4b-25c7-48de-845f-2f887378c7cc.txt'
declare -r LOCAL_TAR='/tmp/a5b63c4b-25c7-48de-845f-2f887378c7cc.tar'
declare -r LOCAL_TAR_GZ='/tmp/a5b63c4b-25c7-48de-845f-2f887378c7cc.tar.gz'
declare -r LOCAL_TGZ='/tmp/a5b63c4b-25c7-48de-845f-2f887378c7cc.tgz'
declare -r LOCAL_TAR_XZ='/tmp/a5b63c4b-25c7-48de-845f-2f887378c7cc.tar.xz'
declare -r LOCAL_TAR_ZSTD='/tmp/a5b63c4b-25c7-48de-845f-2f887378c7cc.tar.zstd'
declare -r VIRT_DIR='/some-dir'
declare -r VIRT_FILE="$VIRT_DIR/my-super-cool-file.txt"
declare -r VIRT_TAR="$VIRT_DIR/my-super-cool-file.tar"
declare -r VIRT_TAR_GZ="$VIRT_DIR/my-super-cool-file.tar.gz"
declare -r VIRT_TGZ="$VIRT_DIR/my-super-cool-file.tgz"
declare -r VIRT_TAR_XZ="$VIRT_DIR/my-super-cool-file.tar.xz"
declare -r VIRT_TAR_ZSTD="$VIRT_DIR/my-super-cool-file.tar.zstd"

run_and_log() {
    echo -e '\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n'
    echo    'Running:' "$@"
    echo -e '-------\n'
    "$@"
}

declare -r DISK_PATH="${1:?'missing disk path'}"
declare -r PARTITION_PATH="${2:?'missing partition path'}"

rm -rf "$SQLITE_DB"
cargo build
echo 'wat' > "$LOCAL_FILE"
tar -cf "$LOCAL_TAR" "$LOCAL_FILE"
tar -czf "$LOCAL_TAR_GZ" "$LOCAL_FILE"
tar -czf "$LOCAL_TGZ" "$LOCAL_FILE"
XZ_OPT='-0' tar -cJf "$LOCAL_TAR_XZ" "$LOCAL_FILE"
tar -I zstd -cf "$LOCAL_TAR_ZSTD" "$LOCAL_FILE"

run_and_log "${hoard[@]}" init
run_and_log "${hoard[@]}" db migrate
run_and_log "${hoard[@]}" db vacuum
run_and_log "${hoard[@]}" collection add "$COLLECTION"
run_and_log "${hoard[@]}" collection ls
run_and_log "${hoard[@]}" disk add --label 'my-disk' "$DISK_PATH"
run_and_log "${hoard[@]}" disk ls 
run_and_log "${hoard[@]}" partition add "$PARTITION_PATH"
run_and_log "${hoard[@]}" partition ls
run_and_log "${hoard[@]}" file add -c "$COLLECTION" "$LOCAL_FILE" "$VIRT_FILE"
run_and_log "${hoard[@]}" file add -c "$COLLECTION" "$LOCAL_TAR" "$VIRT_TAR"
run_and_log "${hoard[@]}" file add -c "$COLLECTION" "$LOCAL_TAR_GZ" "$VIRT_TAR_GZ"
run_and_log "${hoard[@]}" file add -c "$COLLECTION" "$LOCAL_TGZ" "$VIRT_TGZ"
run_and_log "${hoard[@]}" file add -c "$COLLECTION" "$LOCAL_TAR_XZ" "$VIRT_TAR_XZ"
run_and_log "${hoard[@]}" file add -c "$COLLECTION" "$LOCAL_TAR_ZSTD" "$VIRT_TAR_ZSTD"
run_and_log "${hoard[@]}" file ls -c "$COLLECTION" "$VIRT_FILE"
run_and_log "${hoard[@]}" file find -c "$COLLECTION" "$VIRT_DIR"
run_and_log "${hoard[@]}" file inspect -c "$COLLECTION" "$VIRT_FILE"

echo -e '\n'
echo 'The smoke test went happily :)'
