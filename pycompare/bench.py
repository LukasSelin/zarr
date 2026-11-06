"""Time with zarr-python what main.go times with package zarr.

    python bench.py --stores memory,dir --codecs none,zstd --data random,smooth --runs 5

It runs under zarr-python 2 or 3, takes the flags main.go takes, and prints
the same lines of JSON: a write is creating the array and writing all of it
into a store that holds nothing yet, a read is opening it and reading all of
it back. Each run's read is checked against what was written, outside the
time, and the run's array is deleted before the next. compare.py runs both
and sets them side by side; see README.md.

zarr-python 2 writes Zarr version 2 and zarr-python 3 writes version 3, each
the format it writes by default, with the codec asked for and nothing else.
"""

import argparse
import gc
import json
import os
import platform
import shutil
import sys
import tempfile
import time
import zlib

import numcodecs
import numpy as np
import zarr

V3 = int(zarr.__version__.split(".")[0]) >= 3
LIB = "python"


def mix(x):
    """splitmix64's finaliser of x+golden ratio, as mix in main.go."""
    z = x * np.uint64(0x9E3779B97F4A7C15)
    z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return z ^ (z >> np.uint64(31))


def make_data(kind, shape):
    """The array makeData in main.go makes, to the bit; see there."""
    d0, d1, d2 = shape
    n = d0 * d1 * d2
    out = np.empty(n, dtype=np.float64)
    step = 1 << 22
    for lo in range(0, n, step):
        hi = min(lo + step, n)
        i = np.arange(lo, hi, dtype=np.uint64)
        h = mix(i + np.uint64(1))
        if kind == "random":
            out[lo:hi] = (h >> np.uint64(11)).astype(np.float64) * 2.0**-53
        elif kind == "smooth":
            i = i.astype(np.int64)
            z = i % d2
            y = (i // d2) % d1
            x = i // (d1 * d2)
            r = ((x - d0 // 2) ** 2 + (y - d1 // 2) ** 2) >> 6
            q = r + z + (h & np.uint64(255)).astype(np.int64)
            out[lo:hi] = q.astype(np.float64) * 0.1
        else:
            sys.exit(f"bench.py: no data {kind!r}: random or smooth")
    return out.reshape(shape)


def compressor(name):
    """The codec of each name, as codecsOf in main.go makes it."""
    if name == "none":
        return None
    if name == "zstd":
        return zarr.codecs.ZstdCodec(level=0, checksum=False) if V3 else numcodecs.Zstd(level=0)
    if name == "gzip":
        return zarr.codecs.GzipCodec(level=5) if V3 else numcodecs.GZip(level=5)
    sys.exit(f"bench.py: no codec {name!r}: none, zstd or gzip")


def s3_options(args):
    opts = {}
    if args.endpoint:
        opts["endpoint_url"] = args.endpoint
    if args.region:
        opts["client_kwargs"] = {"region_name": args.region}
    return opts


def open_store(kind, name, args):
    """A store that holds nothing yet, where it is, and what deletes it."""
    if kind == "memory":
        return zarr.storage.MemoryStore(), None, lambda: None
    if kind == "dir":
        where = os.path.join(args.dir, name + ".zarr")
        store = zarr.storage.LocalStore(where) if V3 else zarr.storage.DirectoryStore(where)
        return store, where, lambda: shutil.rmtree(where, ignore_errors=True)
    if kind == "s3":
        import s3fs

        path = f"{args.bucket}/{args.prefix}/{name}"
        opts = s3_options(args)
        if V3:
            store = zarr.storage.FsspecStore.from_url("s3://" + path, storage_options=opts)
        else:
            store = zarr.storage.FSStore("s3://" + path, **opts)
        return store, None, lambda: s3fs.S3FileSystem(**opts).rm(path, recursive=True)
    sys.exit(f"bench.py: no store {kind!r}: memory, dir or s3")


def create(store, shape, chunks, codec):
    if V3:
        compressors = None if codec is None else [codec]
        return zarr.create_array(
            store, name="a", shape=shape, chunks=chunks, dtype="float64", compressors=compressors, fill_value=0.0
        )
    return zarr.create(shape=shape, chunks=chunks, dtype="f8", compressor=codec, fill_value=0.0, store=store, path="a")


def open_(store):
    if V3:
        return zarr.open_array(store, path="a", mode="r")
    return zarr.open_array(store=store, mode="r", path="a")


def du(where):
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(where) for f in fs)


def once(store_kind, codec, name, data, args):
    store, where, delete = open_store(store_kind, name, args)
    try:
        gc.collect()
        t = time.perf_counter()
        a = create(store, data.shape, args.chunks, compressor(codec))
        a[...] = data
        write = time.perf_counter() - t

        gc.collect()
        t = time.perf_counter()
        got = open_(store)[...]
        read = time.perf_counter() - t
        if not np.array_equal(got, data):
            sys.exit("bench.py: the array read is not the array written")
        del got
        stored = du(where) if where else None
    finally:
        delete()
    return write, read, stored


def sizes(s):
    out = tuple(int(f) for f in s.split(",") if f.strip())
    if len(out) != 3 or min(out) <= 0:
        raise argparse.ArgumentTypeError(f"{s!r} is not 3 sizes")
    return out


def names(s):
    return [f.strip() for f in s.split(",") if f.strip()]


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--stores", type=names, default="memory,dir", help="of memory, dir and s3")
    p.add_argument("--codecs", type=names, default="none,zstd", help="of none, zstd and gzip")
    p.add_argument("--data", type=names, default="random", help="of random and smooth")
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--dir", default="", help="directory the dir store writes under")
    p.add_argument("--bucket", default="")
    p.add_argument("--prefix", default="pycompare")
    p.add_argument("--endpoint", default="", help="endpoint of an S3-compatible store such as MinIO")
    p.add_argument("--region", default="")
    p.add_argument("--concurrency", type=int, default=0, help="zarr-python 3's async.concurrency; 0 is its default")
    p.add_argument("--shape", type=sizes, default="512,512,512")
    p.add_argument("--chunks", type=sizes, default="512,512,8")
    args = p.parse_args()

    if "s3" in args.stores and not args.bucket:
        sys.exit("bench.py: the s3 store needs --bucket")
    if args.concurrency and V3:
        zarr.config.set({"async.concurrency": args.concurrency})
    made_dir = False
    if not args.dir and "dir" in args.stores:
        args.dir, made_dir = tempfile.mkdtemp(prefix="pycompare-py-"), True

    concurrency = zarr.config.get("async.concurrency") if V3 else None
    emit(
        kind="meta",
        version=zarr.__version__,
        runtime=f"python {platform.python_version()}, numpy {np.__version__}, numcodecs {numcodecs.__version__}",
        cpus=os.cpu_count(),
        concurrency=concurrency,
        shape=list(args.shape),
        chunks=list(args.chunks),
    )
    tag = "py3" if V3 else "py2"
    try:
        for kind in args.data:
            data = make_data(kind, args.shape)
            emit(kind="data", data=kind, crc32=f"{zlib.crc32(data) & 0xFFFFFFFF:08x}")
            for store in args.stores:
                for codec in args.codecs:
                    for r in range(1, args.runs + 1):
                        write, read, stored = once(store, codec, f"{tag}-{kind}-{codec}-{r}", data, args)
                        n = data.nbytes
                        at = dict(kind="run", data=kind, store=store, codec=codec, run=r, bytes=n)
                        if stored:
                            at["stored_bytes"] = stored
                        emit(op="write", seconds=write, **at)
                        emit(op="read", seconds=read, **at)
                        print(
                            f"zarr-python {zarr.__version__} {kind} {store} {codec} run {r}: "
                            f"write {write:.2f} s, read {read:.2f} s",
                            file=sys.stderr,
                            flush=True,
                        )
            del data
    finally:
        if made_dir:
            shutil.rmtree(args.dir, ignore_errors=True)


def emit(**fields):
    print(json.dumps({"kind": fields.pop("kind"), "lib": LIB, **fields}), flush=True)


if __name__ == "__main__":
    main()
