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
declare -r LOCAL_ARCHIVE='/tmp/a5b63c4b-25c7-48de-845f-2f887378c7cc.tar'
declare -r VIRT_DIR='/some-dir'
declare -r VIRT_FILE="$VIRT_DIR/my-super-cool-file.txt"
declare -r VIRT_ARCHIVE="$VIRT_DIR/my-super-cool-file.tar"

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
tar -cf "$LOCAL_ARCHIVE" "$LOCAL_FILE"

run_and_log "${hoard[@]}" init
run_and_log "${hoard[@]}" db migrate
run_and_log "${hoard[@]}" db vacuum
run_and_log "${hoard[@]}" collection add "$COLLECTION"
run_and_log "${hoard[@]}" collection ls
run_and_log "${hoard[@]}" disk add --label 'my-disk' "$DISK_PATH"
run_and_log "${hoard[@]}" disk ls 
run_and_log "${hoard[@]}" partition add "$PARTITION_PATH"
run_and_log "${hoard[@]}" partition ls

declare COLLECTION_ID
COLLECTION_ID="$(sqlite3 "$SQLITE_DB" 'select hex(id) from collections limit 1')"
declare -r COLLECTION_ID

run_and_log "${hoard[@]}" file add -c "$COLLECTION_ID" "$LOCAL_FILE" "$VIRT_FILE"
run_and_log "${hoard[@]}" file add -c "$COLLECTION_ID" "$LOCAL_ARCHIVE" "$VIRT_ARCHIVE"
run_and_log "${hoard[@]}" file ls -c "$COLLECTION_ID" "$VIRT_FILE"
run_and_log "${hoard[@]}" file find -c "$COLLECTION_ID" "$VIRT_DIR"
run_and_log "${hoard[@]}" file inspect -c "$COLLECTION_ID" "$VIRT_FILE"

echo -e '\n'
echo 'The smoke test went happily :)'
