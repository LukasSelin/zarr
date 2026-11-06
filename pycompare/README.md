# pycompare

What the [zarr-python 3 release](https://zarr.dev/blog/zarr-python-3-release/)
timed, with package zarr beside zarr-python 2 and 3: the same array, the
same codec and the same bytes in each.

The release wrote and read a float64 array of 1 GiB, of shape
(512, 512, 512) in chunks of (512, 512, 8), to and from S3, from an
m6i.4xlarge in the bucket's region. Read off its chart, zarr-python 2.18.4
took about 7.2 s to write it and 3.2 s to read it, and 3.0.0 about 5.4 s and
2.05 s. It does not say which codec it used, nor what data.

Results are in [BENCHMARKS.md](../BENCHMARKS.md#against-zarr-python).

## What it times

- **A write** is creating the array and writing all of it, into a store that
  holds nothing yet. **A read** is opening that array and reading all of it.
  Each read is checked against what was written, outside the time, and the
  array is deleted before the next run.
- **The data** is `random`, uniform numbers from 0 to 1, which no codec
  compresses, or `smooth`, a field measured to a tenth with noise on it,
  which zstd and gzip store in about a third of its size. `main.go` and
  `bench.py` make it from integers, so both make the same bits: each prints
  the data's CRC-32, and `compare.py` says if they differ.
- **The codecs** are `none`, `zstd` at level 0 (zstd's own default, 3, and
  zarr-python 3's default compressor) and `gzip` at level 5. zarr-python 2
  writes Zarr version 2, as it does by default, and the others version 3.
- **The stores** are `memory`, `dir`, and `s3`, which takes `--endpoint` for
  MinIO or another S3-compatible store.
- **Concurrency** is each library's default: package zarr works on 16 chunks
  at once (`Array.Concurrency`); zarr-python 3 has 10 in flight
  (`async.concurrency`) and decodes in a thread pool; zarr-python 2 reads
  and writes a memory or dir store one chunk at a time. `--concurrency` sets
  the first two.

## Running it

It needs Go and, for each zarr-python, a Python with it, numpy and s3fs.
zarr-python 2.18.4 does not import with numcodecs 0.16 or later.

```sh
python -m venv .venv3 && .venv3/bin/pip install zarr numpy s3fs
python -m venv .venv2 && .venv2/bin/pip install zarr==2.18.4 "numcodecs<0.16" numpy s3fs
python compare.py --python .venv3/bin/python --python .venv2/bin/python
```

On Windows the interpreter is `.venv3\Scripts\python.exe`. That runs the
memory and dir stores, both data sets and `none` and `zstd`, 5 times each,
and writes `out/results.md` and `out/results.jsonl`. Each flag after
`--python` goes to all three as it is:

```sh
python compare.py --python .venv3/bin/python --codecs none,zstd,gzip --data smooth --runs 3
```

### S3, as the release did

The bucket must exist, and the AWS configuration of the environment - a
profile, `AWS_REGION`, the instance's role - is what each library signs
with. Each run writes under `pycompare/` in the bucket and deletes what it
wrote.

```sh
python compare.py --python .venv3/bin/python --python .venv2/bin/python \
    --stores s3 --bucket my-bucket --region us-east-1
```

To match the release, run it from an m6i.4xlarge in the bucket's region.
From anywhere else the network is what is timed.

### MinIO

```sh
docker run -d -p 9000:9000 minio/minio server /data
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
aws --endpoint-url http://localhost:9000 s3 mb s3://bench
python compare.py --python .venv3/bin/python --stores s3 --bucket bench --endpoint http://localhost:9000
```

A MinIO on the same machine answers in well under a millisecond, so it
times each library's HTTP client and the copies around it, not how well it
hides a round trip to S3.

## Each on its own

`main.go` and `bench.py` take the same flags and print the same lines of
JSON, one for each write and each read:

```sh
go run . -stores memory -codecs zstd -data smooth -runs 3
.venv3/bin/python bench.py --stores memory --codecs zstd --data smooth --runs 3
```
