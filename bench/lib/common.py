"""Helpers shared by the workload runners: the tree's GOEXPERIMENT, harness placement, and the readers built once
from the base tree that check a store after a run (so neither side's code reads its own output)."""
import os
import shutil
import subprocess
import tempfile

LIB = os.path.dirname(os.path.abspath(__file__))
SEIDB = "seidb"
RECEIPT_READER = "receiptreader"


def tree_goexperiment(tree):
    """The GOEXPERIMENT the tree's own Makefile builds seid with (empty when it sets none)."""
    d = tempfile.mkdtemp(prefix="bench-goexp-")
    mk = os.path.join(d, "goexp.mk")
    with open(mk, "w") as f:
        f.write('bench-print-goexperiment:\n\t@echo "$(GOEXPERIMENT)"\n')
    env = {k: v for k, v in os.environ.items() if k != "GOEXPERIMENT"}
    try:
        return subprocess.run(["make", "--no-print-directory", "-s", "-f", "Makefile", "-f", mk, "bench-print-goexperiment"],
                              cwd=tree, env=env, capture_output=True, text=True, check=True).stdout.strip()
    finally:
        shutil.rmtree(d, ignore_errors=True)


def go_env(tree):
    """os.environ with the tree's GOEXPERIMENT set (or cleared)."""
    goexp = tree_goexperiment(tree)
    env = {k: v for k, v in os.environ.items() if k != "GOEXPERIMENT"}
    if goexp:
        env["GOEXPERIMENT"] = goexp
    print(f"goexperiment={goexp or '<none>'}")
    return env


def place_harness(src, tree, rel_dir, name):
    """Copies one harness file into the tree's package directory. The file carries the benchharness build tag, so
    only a build with -tags benchharness compiles it; it is never committed to either ref."""
    dst = os.path.join(tree, rel_dir, name)
    shutil.copyfile(src, dst)
    return dst


def build_tools(base_tree, out):
    """Builds, from the base tree, `seidb` (dump-flatkv recomputes a FlatKV store's LtHash) and `receiptreader`
    (digests over a littidx receipt store's receipts and logs). Both sides' stores are read by the same binaries."""
    os.makedirs(out, exist_ok=True)
    env = go_env(base_tree)
    seidb = os.path.join(out, SEIDB)
    if not os.access(seidb, os.X_OK):
        subprocess.run(["go", "build", "-o", seidb, "./sei-db/tools/cmd/seidb"], cwd=base_tree, env=env, check=True)
    reader = os.path.join(out, RECEIPT_READER)
    if not os.access(reader, os.X_OK):
        pkg = os.path.join(base_tree, "bench-receipt-reader")
        os.makedirs(pkg, exist_ok=True)
        shutil.copyfile(os.path.join(LIB, "receiptreader", "main.go.txt"), os.path.join(pkg, "main.go"))
        try:
            subprocess.run(["go", "build", "-o", reader, "./bench-receipt-reader"], cwd=base_tree, env=env, check=True)
        finally:
            shutil.rmtree(pkg, ignore_errors=True)
    print(f"tools: {seidb} {reader}")
    return seidb, reader


def apply_witness(figures, witness, tolerance, declared):
    """A second, independent count of a figure (e.g. the same span on this wrapper's own clock) must agree with it
    within `tolerance` (relative). A figure in `declared` whose witness is missing or disagrees is set to None, so
    the comparison leaves that reading out; the returned list says which and why."""
    disputes = []
    for name in declared:
        row = figures.get(name)
        stated = witness.get(name)
        if not isinstance(stated, (int, float)) or isinstance(stated, bool):
            disputes.append({"figure": name, "figure_value": row, "witness": stated, "reason": "witness absent"})
            figures[name] = None
            continue
        if isinstance(row, (int, float)) and not isinstance(row, bool):
            gap = abs(float(stated) - float(row)) / max(abs(float(row)), 1e-12)
            if gap > tolerance:
                disputes.append({"figure": name, "figure_value": row, "witness": stated, "gap": gap, "reason": "witness disagrees"})
                figures[name] = None
    return disputes
