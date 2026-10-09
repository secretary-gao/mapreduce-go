#!/bin/bash
# Benchmark the MapReduce implementation (word count).
#
#   bench/bench.sh build            build race-free binaries into bench/bin
#   bench/bench.sh data             create the "small" and "large" input sets
#   bench/bench.sh run [reps]       run all configurations, print a CSV table
#
# Every distributed run is verified against the output of mrsequential.

set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bench/bin"
TMP="$ROOT/bench/tmp"
REPS="${2:-3}"

now() { date +%s.%N; }

build() {
	mkdir -p "$BIN"
	cd "$ROOT"
	go build -o "$BIN/mrcoordinator" main/mrcoordinator.go
	go build -o "$BIN/mrworker" main/mrworker.go
	go build -o "$BIN/mrsequential" main/mrsequential.go
	go build -buildmode=plugin -o "$BIN/wc.so" mrapps/wc.go
	echo "built into $BIN"
}

data() {
	rm -rf "$TMP/data-small" "$TMP/data-large"
	mkdir -p "$TMP/data-small" "$TMP/data-large"
	cp "$ROOT"/main/pg-*.txt "$TMP/data-small/"
	for f in "$ROOT"/main/pg-*.txt; do
		b=$(basename "$f" .txt)
		for k in 1 2 3 4 5 6 7 8 9 10; do
			cp "$f" "$TMP/data-large/$b-$k.txt"
		done
	done
	echo "small: $(ls "$TMP/data-small" | wc -l) files, $(du -sh "$TMP/data-small" | cut -f1)"
	echo "large: $(ls "$TMP/data-large" | wc -l) files, $(du -sh "$TMP/data-large" | cut -f1)"
}

# reference output of the sequential implementation for one data set
reference() {
	local set=$1
	local dir="$TMP/ref-$set"
	rm -rf "$dir"; mkdir -p "$dir"
	cd "$dir"
	"$BIN/mrsequential" "$BIN/wc.so" "$TMP"/data-$set/*.txt
	sort mr-out-0 > "$TMP/ref-$set.sorted"
}

run_seq() {
	local set=$1
	local dir="$TMP/run-seq"
	rm -rf "$dir"; mkdir -p "$dir"
	cd "$dir"
	local t0=$(now)
	"$BIN/mrsequential" "$BIN/wc.so" "$TMP"/data-$set/*.txt
	local t1=$(now)
	sort mr-out-0 | cmp -s - "$TMP/ref-$set.sorted" && ok=ok || ok=WRONG
	printf "%s,sequential,0,%.2f,%.2f,%s\n" "$set" "$(echo "$t1 - $t0" | bc -l)" \
		"$(echo "$t1 - $t0" | bc -l)" "$ok"
}

run_dist() {
	local set=$1 n=$2
	local dir="$TMP/run-dist"
	rm -rf "$dir"; mkdir -p "$dir"
	cd "$dir"
	local sock="/tmp/mr-bench-$$"
	rm -f "$sock"
	local t0=$(now)
	"$BIN/mrcoordinator" "$sock" "$TMP"/data-$set/*.txt > /dev/null 2>&1 &
	local cpid=$!
	while [ ! -S "$sock" ]; do sleep 0.02; done
	local wpids=""
	for ((i = 0; i < n; i++)); do
		"$BIN/mrworker" "$BIN/wc.so" "$sock" > /dev/null 2>&1 &
		wpids="$wpids $!"
	done
	wait $cpid
	local t1=$(now)
	for p in $wpids; do kill $p 2> /dev/null; done
	wait 2> /dev/null
	rm -f "$sock"
	cat mr-out-* | sort | cmp -s - "$TMP/ref-$set.sorted" && ok=ok || ok=WRONG
	# true job time: when the last output file was finalized. The process
	# exit time is rounded up to whole seconds by mrcoordinator's Done() poll.
	local last=$(find . -maxdepth 1 -name 'mr-out-*' -printf '%T@\n' | sort -n | tail -1)
	printf "%s,distributed,%d,%.2f,%.2f,%s\n" "$set" "$n" \
		"$(echo "$last - $t0" | bc -l)" "$(echo "$t1 - $t0" | bc -l)" "$ok"
}

run() {
	echo "dataset,mode,workers,job_seconds,exit_seconds,correct"
	for set in small large; do
		reference $set
		for ((r = 0; r < REPS; r++)); do run_seq $set; done
		for n in 1 2 4; do
			for ((r = 0; r < REPS; r++)); do run_dist $set $n; done
		done
	done
}

case "${1:-}" in
build) build ;;
data) data ;;
run) run ;;
*) echo "usage: $0 build|data|run [reps]"; exit 1 ;;
esac
