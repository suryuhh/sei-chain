#!/bin/sh
# Downloads the benchmark data files named on the command line (all of them when none is named) into
# BENCH_DATA_DIR (default ~/.cache/sei-bench/data) and verifies each against the sha256 in data/manifest.json.
# A file already present with the right sha256 is not downloaded again.
#
#   BENCH_DATA_URL   base URL the files are published under (a release's asset URL, or file:///path/to/dir);
#                    by default the files are read from bench/data/files, which ships with the kit
set -eu
KIT=$(cd "$(dirname "$0")" && pwd)
MANIFEST="$KIT/data/manifest.json"
DATA=${BENCH_DATA_DIR:-$HOME/.cache/sei-bench/data}
mkdir -p "$DATA"

names=$(python3 - "$MANIFEST" "$@" <<'PY'
import json, sys
man = json.load(open(sys.argv[1]))
known = {f["name"] for f in man["files"]}
want = sys.argv[2:] or sorted(known)
bad = [n for n in want if n not in known]
if bad:
    sys.exit("unknown data file(s): " + " ".join(bad))
print(" ".join(want))
PY
)
url=${BENCH_DATA_URL:-$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["url"])' "$MANIFEST")}

sha_of() { python3 -c 'import hashlib,sys
h=hashlib.sha256()
with open(sys.argv[1],"rb") as f:
    for c in iter(lambda: f.read(1<<20), b""): h.update(c)
print(h.hexdigest())' "$1"; }
want_of() { python3 -c 'import json,sys
print({f["name"]: f["sha256"] for f in json.load(open(sys.argv[1]))["files"]}[sys.argv[2]])' "$MANIFEST" "$1"; }

for n in $names; do
  want=$(want_of "$n")
  if [ -f "$DATA/$n" ] && [ "$(sha_of "$DATA/$n")" = "$want" ]; then
    echo "data $n: present, sha256 verified"
    continue
  fi
  if [ -z "${BENCH_DATA_URL:-}" ] && [ -f "$KIT/data/files/$n" ]; then
    [ "$(sha_of "$KIT/data/files/$n")" = "$want" ] || { echo "data $n: the copy in bench/data/files does not match its sha256" >&2; exit 1; }
    cp "$KIT/data/files/$n" "$DATA/$n"; echo "data $n: from bench/data/files, sha256 verified"; continue
  fi
  case "$url" in
    file://bench/*|*BENCH_DATA_URL*|"") echo "data $n is missing from bench/data/files and no BENCH_DATA_URL is set" >&2; exit 1 ;;
  esac
  echo "data $n: downloading from $url/$n"
  curl -fsSL --retry 3 -o "$DATA/$n.part" "$url/$n"
  got=$(sha_of "$DATA/$n.part")
  if [ "$got" != "$want" ]; then
    rm -f "$DATA/$n.part"
    echo "data $n: sha256 $got is not the published $want" >&2
    exit 1
  fi
  mv "$DATA/$n.part" "$DATA/$n"
  echo "data $n: sha256 verified"
done
