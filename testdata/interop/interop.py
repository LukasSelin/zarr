"""Interop with zarr-python: see interop_test.go.

    python interop.py write DIR   write every case with zarr-python
    python interop.py read DIR    read every case zarr.Go wrote, and check it

Both sides make each array's data the same way: element i of the array in
C order is value(dtype, i), and the elements of the first chunk are the fill
value, so that chunk is never stored and must read as the fill.
"""

import json
import math
import pathlib
import sys

import numpy as np
import zarr
from zarr.codecs import BytesCodec, Crc32cCodec, GzipCodec, ShardingCodec
from zarr.codecs.numcodecs import Shuffle

HERE = pathlib.Path(__file__).parent
CASES = json.loads((HERE / "cases.json").read_text())
SEED = 2**64 - 1


def fill_of(case):
    f = case["fill"]
    if isinstance(f, str):
        return {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}[f]
    return f


def compressor_of(name, dtype):
    if name == "gzip":
        return GzipCodec(level=5)
    if name == "shuffle":
        return Shuffle(elementsize=np.dtype(dtype).itemsize)
    return Crc32cCodec()


def expected(case):
    shape, dt = tuple(case["shape"]), case["dtype"]
    i = np.arange(math.prod(shape), dtype=np.int64)
    if dt == "bool":
        v = i % 3 == 0
    elif dt == "uint64":
        v = np.uint64(2**64 - 1) - i.astype(np.uint64)
    elif dt.startswith("uint"):
        v = (i * 7) % 251
    elif dt.startswith("int"):
        v = (i * 7) % 200 - 100
    else:
        v = i * 0.25 - 3
    a = v.astype(dt).reshape(shape)
    if shape:
        a[tuple(slice(0, c) for c in case["chunks"])] = fill_of(case)
    return a


def write(path):
    root = zarr.open_group(path, mode="w", zarr_format=3, attributes={"seed": SEED, "name": "terra"})
    for case in CASES:
        name = case["name"]
        if "/" in name:
            root.require_group(name.rsplit("/", 1)[0])
        compressors = [compressor_of(c, case["dtype"]) for c in case["compressors"]]
        serializer = BytesCodec(endian=case["endian"])
        chunks, shards = tuple(case["chunks"]), None
        sharding = case.get("sharding")
        if sharding and sharding.get("after"):
            # Codecs after the shard: zarr-python takes these only as a
            # sharding serializer with the shard as its chunk.
            serializer = ShardingCodec(
                chunk_shape=chunks,
                codecs=[serializer, *compressors],
                index_location=sharding["index_location"],
            )
            chunks, compressors = tuple(sharding["shape"]), [GzipCodec(level=5) for _ in sharding["after"]]
        elif sharding:
            shards = {"shape": tuple(sharding["shape"]), "index_location": sharding["index_location"]}
        arr = root.create_array(
            name,
            shape=tuple(case["shape"]),
            chunks=chunks,
            shards=shards,
            dtype=case["dtype"],
            fill_value=fill_of(case),
            serializer=serializer,
            compressors=compressors,
            filters=[],
            chunk_key_encoding={"name": "default", "separator": case["separator"]},
            dimension_names=case.get("dimension_names"),
        )
        arr[...] = expected(case)
    # A Zarr version 2 group and array beside the rest, which zarr.Go must
    # refuse as version 2 rather than as not there.
    v2 = zarr.open_group(pathlib.Path(path) / "v2", mode="w", zarr_format=2, attributes={"format": 2})
    v2.create_array("a", shape=(5,), chunks=(2,), dtype="int32", fill_value=0)[...] = np.arange(5, dtype="int32")
    print(f"wrote {len(CASES)} arrays, and a version 2 group")


def read(path):
    root = zarr.open_group(path, mode="r")
    failures = []
    if root.attrs.get("seed") != SEED:
        failures.append(f"seed attribute is {root.attrs.get('seed')!r}")
    for case in CASES:
        name = case["name"]
        try:
            arr = root[name]
            got, want = arr[...], expected(case)
            if got.dtype != want.dtype:
                failures.append(f"{name}: dtype {got.dtype}, want {want.dtype}")
            elif not np.array_equal(got, want, equal_nan=want.dtype.kind == "f"):
                failures.append(f"{name}: data\n got {got!r}\nwant {want!r}")
            sharding = case.get("sharding") or {}
            if sharding.get("after"):
                # zarr-python reports the shard as the chunk here.
                codec = arr.metadata.codecs[0]
                if tuple(codec.chunk_shape) != tuple(case["chunks"]) or tuple(arr.chunks) != tuple(sharding["shape"]):
                    failures.append(f"{name}: chunks {codec.chunk_shape} in {arr.chunks}")
            elif tuple(arr.chunks) != tuple(case["chunks"]):
                failures.append(f"{name}: chunks {arr.chunks}")
            elif sharding and tuple(arr.shards) != tuple(sharding["shape"]):
                failures.append(f"{name}: shards {arr.shards}")
            if sharding:
                location = arr.metadata.codecs[0].index_location
                if str(getattr(location, "value", location)) != sharding["index_location"]:
                    failures.append(f"{name}: index location {location}")
            names = case.get("dimension_names")
            if names is not None and list(arr.metadata.dimension_names) != names:
                failures.append(f"{name}: dimension names {arr.metadata.dimension_names}")
            if case["fill"] == "NaN":
                ok = math.isnan(arr.fill_value)
            else:
                ok = arr.fill_value == fill_of(case)
            if not ok:
                failures.append(f"{name}: fill {arr.fill_value!r}")
        except Exception as e:  # report every case, not just the first
            failures.append(f"{name}: {type(e).__name__}: {e}")
    if failures:
        print("\n".join(failures))
        sys.exit(1)
    print(f"read {len(CASES)} arrays")


if __name__ == "__main__":
    {"write": write, "read": read}[sys.argv[1]](sys.argv[2])
