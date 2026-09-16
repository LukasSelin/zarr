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
from zarr.codecs import BytesCodec, Crc32cCodec, GzipCodec

HERE = pathlib.Path(__file__).parent
CASES = json.loads((HERE / "cases.json").read_text())
SEED = 2**64 - 1


def fill_of(case):
    f = case["fill"]
    if isinstance(f, str):
        return {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}[f]
    return f


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
        compressors = [GzipCodec(level=5) if c == "gzip" else Crc32cCodec() for c in case["compressors"]]
        arr = root.create_array(
            name,
            shape=tuple(case["shape"]),
            chunks=tuple(case["chunks"]),
            dtype=case["dtype"],
            fill_value=fill_of(case),
            serializer=BytesCodec(endian=case["endian"]) if case["endian"] else BytesCodec(endian=None),
            compressors=compressors,
            filters=[],
            chunk_key_encoding={"name": "default", "separator": case["separator"]},
            dimension_names=case.get("dimension_names"),
        )
        arr[...] = expected(case)
    print(f"wrote {len(CASES)} arrays")


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
            if tuple(arr.chunks) != tuple(case["chunks"]):
                failures.append(f"{name}: chunks {arr.chunks}")
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
