# Shopping Sessions: native C++ pipeline

One static C++17 binary (`src/main.cpp`, no dependencies) that implements `rules.md`:
reads `$DATA_DIR`, writes `$OUTPUT_DIR/user_daily/*.parquet` and
`$OUTPUT_DIR/campaign_daily/*.parquet`. Design notes are in [PLAN.md](PLAN.md).

## Build and run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
scripts/docker_run.sh /path/to/data out          # scorer flags (from the starter)
```

Without Docker (Linux or macOS, clang or g++):

```sh
c++ -std=c++17 -O2 -pthread -o ss src/main.cpp
DATA_DIR=/path/to/data OUTPUT_DIR=out ./ss
```

## Tests

```sh
python3 scripts/run_fixtures.py fixtures -- ./ss          # 36 edge-case fixtures
python3 scripts/compare.py out dev-answer                 # exact diff vs the published dev answer
python3 scripts/reference.py DATA ref && python3 scripts/compare.py out ref   # any seed
```

`scripts/reference.py` is a slow, independent pure-Python implementation of the rules, used
to cross-check other seeds (it matches the published dev answer and every fixture).
`scripts/vrun.sh` / `scripts/bench.sh` run the image on datasets kept in a Docker volume.

## How it is fast

- mmap every CSV (with a zero page after each file, so SIMD scans and hash loads may over-read),
  split into 4 MB chunks on a work queue over `NCPU` threads.
- SIMD (AVX2 / NEON) scan for `,` `"` `\n`; quoting validated once per line by counting quotes.
- SWAR timestamp parsing; 128-bit hashes for IDs; a lock-free hash table maps user IDs to
  slots, with lock-free union-find for linked IDs done while the links are parsed.
- Events are partitioned by event-id hash for dedup, then by person; each person's events are
  sorted as packed 64-bit keys and walked once for sessions, credit and assists.
- A minimal hand-written Parquet writer (PLAIN, uncompressed), one file per thread.
