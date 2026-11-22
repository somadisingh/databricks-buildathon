#!/bin/sh
# Local perf loop: run IMAGE on a dataset stored in docker volume ssdata (subdir $1), scorer flags.
set -eu
IMAGE="${IMAGE:-ss:arm}"; SET="$1"; OUT="${2:-out_$1}"
# fresh dir name per run: Docker Desktop's virtiofs caches a deleted-and-recreated dir badly
rm -rf "$OUT"; RUN="$OUT.$$"; mkdir -p "$RUN"; chmod 777 "$RUN"
trap 'mv "$RUN" "$OUT"' EXIT
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
  --cpus "${CPUS:-8}" --memory "${MEM:-7g}" --memory-swap "${MEM:-7g}" --user 1000:1000 \
  --mount type=volume,src=ssdata,dst=/data,volume-subpath="$SET",readonly \
  -v "$PWD/$RUN":/output --tmpfs /scratch:rw,size=1g,mode=1777 \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 \
  "$IMAGE"
