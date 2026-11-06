"""Run main.go and bench.py with the same flags and set the results side by side.

    python compare.py --python venv3/bin/python --python venv2/bin/python

Each --python is an interpreter with a zarr-python installed, along with
numpy, and s3fs for the s3 store; bench.py runs under each. main.go is built
and run once beside them. Every other flag is passed to all of them as it is.
The lines they print go into OUT/results.jsonl, and the tables into
OUT/results.md and onto the terminal. This script needs nothing but Python.
"""

import argparse
import json
import os
import pathlib
import platform
import statistics
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent

# The flags main.go and bench.py both take, with compare.py's defaults.
SHARED = {
    "stores": "memory,dir",
    "codecs": "none,zstd",
    "data": "random,smooth",
    "runs": "5",
    "dir": "",
    "bucket": "",
    "prefix": "pycompare",
    "endpoint": "",
    "region": "",
    "concurrency": "0",
    "shape": "512,512,512",
    "chunks": "512,512,8",
}


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--python", action="append", default=[], help="a Python with zarr-python; may be given more than once")
    p.add_argument("--no-go", action="store_true", help="leave main.go out")
    p.add_argument("--out", default=str(HERE / "out"), help="directory for results.jsonl and results.md")
    for name, default in SHARED.items():
        p.add_argument("--" + name, default=default)
    args = p.parse_args()

    shared = [(k, getattr(args, k)) for k in SHARED if getattr(args, k) != ""]
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    lines = []
    if not args.no_go:
        exe = out / ("pycompare.exe" if os.name == "nt" else "pycompare")
        subprocess.run(["go", "build", "-o", str(exe), "."], cwd=HERE, check=True)
        lines += run([str(exe)] + [f"-{k}={v}" for k, v in shared])
    for py in args.python:
        lines += run([py, str(HERE / "bench.py")] + [f"--{k}={v}" for k, v in shared])

    with open(out / "results.jsonl", "w", encoding="utf-8") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")
    md = report(lines)
    (out / "results.md").write_text(md, encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
    print(md)


def run(cmd):
    """Run one benchmark, its progress to the terminal, and return its lines,
    each with the name of the library that made it."""
    print("==>", " ".join(cmd), file=sys.stderr, flush=True)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, encoding="utf-8")
    lines, label = [], None
    for raw in proc.stdout:
        line = json.loads(raw)
        if line["kind"] == "meta":
            label = "Go zarr" if line["lib"] == "go" else f"zarr-python {line['version']}"
        line["label"] = label
        lines.append(line)
    if proc.wait() != 0:
        sys.exit(f"compare.py: {cmd[0]} failed")
    return lines


def order(label):
    """zarr-python oldest first, then Go: the blog's order, and then ours."""
    if label == "Go zarr":
        return (1, ())
    return (0, tuple(int(x) if x.isdigit() else 0 for x in label.split()[-1].split(".")))


def report(lines):
    metas = {l["label"]: l for l in lines if l["kind"] == "meta"}
    labels = sorted(metas, key=order)
    runs = [l for l in lines if l["kind"] == "run"]
    newest = [x for x in labels if x != "Go zarr"]
    vs = newest[-1] if newest and "Go zarr" in labels else None

    out = ["# Package zarr against zarr-python", ""]
    out.append(f"{platform.platform()}, {platform.processor() or platform.machine()}, {os.cpu_count()} CPUs.")
    out.append("")
    for label in labels:
        m = metas[label]
        conc = m.get("concurrency")
        conc = "default" if not conc and label == "Go zarr" else conc
        conc = f", concurrency {conc}" if conc is not None else ""
        version = f" ({m['version']})" if label == "Go zarr" and m.get("version") else ""
        out.append(f"- {label}{version}: {m['runtime']}{conc}")
    first = next(iter(metas.values()))
    out.append("")
    out.append(
        f"A float64 array of shape {tuple(first['shape'])} in chunks of {tuple(first['chunks'])}; "
        "each cell is the median time of the runs, with the fastest and slowest after it."
    )

    crcs = {}
    for l in lines:
        if l["kind"] == "data":
            crcs.setdefault(l["data"], set()).add(l["crc32"])
    for data, got in crcs.items():
        if len(got) > 1:
            out += ["", f"**The {data} data differ between libraries ({', '.join(sorted(got))}): the runs are not comparable.**"]

    keys = []
    for r in runs:
        k = (r["data"], r["store"], r["codec"], r["op"])
        if k not in keys:
            keys.append(k)
    groups = []
    for d, s, _, _ in keys:
        if (d, s) not in groups:
            groups.append((d, s))

    for data, store in groups:
        out += ["", f"## {data} data, {store} store", ""]
        head = ["codec", "op"] + labels + ([f"Go against {vs}"] if vs else [])
        out.append("| " + " | ".join(head) + " |")
        out.append("|" + "---|" * 2 + "---:|" * (len(head) - 2))
        for d, s, codec, op in keys:
            if (d, s) != (data, store):
                continue
            row = [codec, op]
            med = {}
            for label in labels:
                ts = [r["seconds"] for r in runs if r["label"] == label and (r["data"], r["store"], r["codec"], r["op"]) == (d, s, codec, op)]
                if not ts:
                    row.append("")
                    continue
                med[label] = statistics.median(ts)
                row.append(f"{med[label]:.2f} s ({min(ts):.2f}–{max(ts):.2f})")
            if vs:
                if vs in med and "Go zarr" in med and med["Go zarr"] > 0:
                    x = med[vs] / med["Go zarr"]
                    row.append(f"{x:.2f}× faster" if x >= 1 else f"{1 / x:.2f}× slower")
                else:
                    row.append("")
            out.append("| " + " | ".join(row) + " |")

    sizes = {}
    for r in runs:
        if r.get("stored_bytes") and r["op"] == "write":
            sizes.setdefault((r["data"], r["codec"]), {}).setdefault(r["label"], []).append(r["bytes"] / r["stored_bytes"])
    if sizes:
        out += ["", "## Compression ratio, from the dir store", ""]
        out.append("| data | codec | " + " | ".join(labels) + " |")
        out.append("|---|---|" + "---:|" * len(labels))
        for (data, codec), by in sizes.items():
            out.append(f"| {data} | {codec} | " + " | ".join(f"{statistics.median(by[x]):.2f}" if x in by else "" for x in labels) + " |")
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    main()
