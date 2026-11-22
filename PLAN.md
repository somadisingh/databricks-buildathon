# Plan: Shopping Sessions, native C++

## Why C++ and not DuckDB/Polars/Spark
Score = wall time from container start, including engine start-up. The work is
~2 GB (practice) / ~6 GB (final) of CSV with a custom dialect that no engine's
CSV reader matches (quoted `""` = null, `\r` is data, strict malformed rules), so
an engine would still need a hand parser. A single static C++ binary has ~0 ms
start-up, parses with mmap + 8 threads, and writes Parquet with a ~150-line
hand-rolled writer (PLAIN, uncompressed, Thrift compact footer). No runtime deps.

## Pipeline (all phases parallel over 8 threads unless noted)
0. **Small inputs**: manifest window, products (string -> category idx, categories
   sorted byte-wise with `unknown` inserted), campaigns (string -> idx, channel).
1. **Chunking**: mmap every `*.csv`; split big files into ~8 MB newline-aligned
   chunks; first line of each file is the header and is skipped. Dynamic work queue.
2. **Links**: parse (dialect, 2 fields, both non-empty) -> insert both IDs into a
   global lock-free hash table keyed by a 128-bit hash (slot index = uid), keep a
   pointer to the string bytes in the mmap. Union-find over uids; per root keep
   the byte-wise smallest ID as the person's name.
3. **Events + clicks parse**: per line: split on `,`, validate quoting, field count.
   - event: drop empty `event_id`; compute validity of this copy (user, ts form +
     window, type, product, purchase price) and the late flag; look up/insert user
     -> person; emit 40-byte record into one of 256 partitions by event-id hash.
   - click: drop empty `click_id`; validate fully (user, ts in click window,
     campaign known) and emit `{person, ts, campaign, click_id ptr}` into one of 256
     partitions by person. Click dedup is skipped: copies are identical, so they
     can't change the credited click (max ts, min id) or the distinct assist set.
4. **Dedup** per event partition: hash table on (h1,h2), keep min `ingest_ts`;
   survivors that are valid and not late -> 12-byte `{person, ts, price|cat|type}`
   record into person partitions.
5. **Sessionize + attribute** per person partition: sort events by (person, ts) and
   clicks by (person, ts, click_id); walk each person: 30-min gap rule, session day,
   credit = last click in `[S-7d, S]` (ties: smallest id), assists = distinct
   campaigns in that window kept by a two-pointer sliding window with per-campaign
   counts. Accumulate user_daily rows (per person-day) and campaign_daily counters
   (thread-local `[campaign][day]` arrays, merged at the end).
6. **Write**: each thread writes its own `user_daily/part-T.parquet`; one
   `campaign_daily/part-0.parquet`. `day` = INT32 DATE.

## Correctness traps to cover (all in fixtures)
dedup-before-validate (E5), late cutoff `> 24h` (E6), offsets range/colon (E17),
ASCII digits only / F3 any length & leading zeros (E18), spaces significant (E19),
`\r` is data (E20), equal-ts sessions (E16), click window edges (A2/A4/A10),
assists incl. assist-only rows (A11/A12), link parsing + header-like data rows (L3).

## Phases
1. Correct pipeline, pass all 36 fixtures (run natively on macOS with clang).
2. Match `dev-answer` exactly on the 1M dev set.
3. Generate 20M / 60M sets, profile, optimize (prefetch hash lookups, SIMD line
   scanning, radix sorts, I/O strategy), keep memory well under 24 GB.
4. Dockerfile (multi-stage, static binary, uid 1000-safe), verify amd64 image with
   the starter's scorer-flag `docker_run.sh`, fixtures + check-dev in container.

## Risks
- 128-bit hashes stand in for ID equality (collision odds ~1e-20): accepted.
- Hash-table sizing is estimated from input size; overflow triggers a rebuild.
- Page-cache state on the scorer is unknown; I/O may dominate at 60M.

## Status (2026-11-21)
Phases 1–4 done. Verified: 36/36 fixtures (native, arm64 and amd64 images with scorer flags),
exact MATCH with `dev-answer`, and MATCH against the independent `scripts/reference.py` on 11
other seeds (incl. three windows containing 2028-02-29); clean under ASAN+UBSan; timestamp
fast path fuzzed against the plain parser (`tests/ts_test.cpp`).

Local timings (Apple M4 Pro, Linux arm64 container, 8 CPUs, warm cache), in-process:

| set | time | notes |
| --- | --- | --- |
| dev 1M | ~0.07 s | |
| practice-size 20M | ~0.39 s median | parse 0.20, sessions 0.07, write 0.04, dedup 0.04, links 0.04 |
| final-size 60M | ~1.3 s (macOS native) | peak RSS 11 GB incl. ~6 GB of mapped input |

Expect the 8-vCPU Xeon scorer to be ~2–3× slower than these numbers.
