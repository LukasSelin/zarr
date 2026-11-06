// Command pycompare times what the zarr-python 3 release measured - a write,
// then a read, of a float64 array of 1 GiB, of shape 512 by 512 by 512 in
// chunks of 512 by 512 by 8 - with package zarr, and prints a line of JSON for
// each run. compare.py runs it beside bench.py, which times the same with
// zarr-python 2 and 3, and sets the results side by side. See README.md.
//
//	go run . -stores memory,dir -codecs none,zstd -data random,smooth -runs 5
//
// A write is CreateArray and Write of the whole array, into a store that holds
// nothing yet; a read is OpenArray and Read of the whole array just written.
// Each run's read is checked against what was written, outside the time, and
// the run's array is deleted before the next.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/LukasSelin/zarr"
	zarrs3 "github.com/LukasSelin/zarr/s3"
	"github.com/LukasSelin/zarr/zstd"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// config is what the flags ask for.
type config struct {
	stores, codecs, data []string
	runs                 int
	dir                  string
	bucket, prefix       string
	endpoint, region     string
	concurrency          int
	shape, chunks        []int
}

// The lines of output, each with its kind: one "meta" first, of what every
// run was made with; a "data" for each data set, with its checksum; and a
// "run" for each write and each read.
type (
	meta struct {
		Kind        string `json:"kind"`
		Lib         string `json:"lib"`
		Version     string `json:"version"`
		Runtime     string `json:"runtime"`
		CPUs        int    `json:"cpus"`
		Concurrency int    `json:"concurrency"`
		Shape       []int  `json:"shape"`
		Chunks      []int  `json:"chunks"`
	}
	dataSet struct {
		Kind  string `json:"kind"`
		Lib   string `json:"lib"`
		Data  string `json:"data"`
		CRC32 string `json:"crc32"`
	}
	result struct {
		Kind    string  `json:"kind"`
		Lib     string  `json:"lib"`
		Data    string  `json:"data"`
		Store   string  `json:"store"`
		Codec   string  `json:"codec"`
		Op      string  `json:"op"`
		Run     int     `json:"run"`
		Seconds float64 `json:"seconds"`
		Bytes   int64   `json:"bytes"`
		// Stored is how many bytes the array took in the store, where that
		// is cheap to find: a dir store.
		Stored int64 `json:"stored_bytes,omitempty"`
	}
)

func main() {
	var c config
	stores := flag.String("stores", "memory,dir", "stores to time, of memory, dir and s3")
	codecs := flag.String("codecs", "none,zstd", "codecs to time, of none, zstd and gzip")
	data := flag.String("data", "random", "data to write, of random and smooth")
	flag.IntVar(&c.runs, "runs", 5, "runs of each data set, store and codec")
	flag.StringVar(&c.dir, "dir", "", "directory the dir store writes under; a new one in the temporary directory if empty")
	flag.StringVar(&c.bucket, "bucket", "", "bucket of the s3 store")
	flag.StringVar(&c.prefix, "prefix", "pycompare", "prefix in the bucket the s3 store writes under")
	flag.StringVar(&c.endpoint, "endpoint", "", "endpoint of an S3-compatible store such as MinIO; empty is AWS")
	flag.StringVar(&c.region, "region", "", "region of the bucket; empty is what the AWS configuration says")
	flag.IntVar(&c.concurrency, "concurrency", 0, "Array.Concurrency; 0 is the package's default")
	shape := flag.String("shape", "512,512,512", "shape of the array, of 3 dimensions")
	chunks := flag.String("chunks", "512,512,8", "shape of a chunk")
	flag.Parse()

	c.stores, c.codecs, c.data = split(*stores), split(*codecs), split(*data)
	var err error
	if c.shape, err = ints(*shape); err == nil {
		c.chunks, err = ints(*chunks)
	}
	if err == nil && (len(c.shape) != 3 || len(c.chunks) != 3) {
		err = errors.New("the shape and the chunks have 3 dimensions")
	}
	if err == nil {
		err = run(context.Background(), c, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pycompare:", err)
		os.Exit(1)
	}
}

func split(s string) []string {
	var out []string
	for f := range strings.SplitSeq(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

func ints(s string) ([]int, error) {
	var out []int
	for _, f := range split(s) {
		n, err := strconv.Atoi(f)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("%q is not a list of sizes", s)
		}
		out = append(out, n)
	}
	return out, nil
}

func run(ctx context.Context, c config, w io.Writer) error {
	out := json.NewEncoder(w)
	err := out.Encode(meta{
		Kind: "meta", Lib: "go", Version: version(), Runtime: runtime.Version(),
		CPUs: runtime.GOMAXPROCS(0), Concurrency: c.concurrency, Shape: c.shape, Chunks: c.chunks,
	})
	if err != nil {
		return err
	}
	var client *awss3.Client
	if slices.Contains(c.stores, "s3") {
		if c.bucket == "" {
			return errors.New("the s3 store needs -bucket")
		}
		if client, err = s3Client(ctx, c); err != nil {
			return err
		}
	}
	if c.dir == "" && slices.Contains(c.stores, "dir") {
		if c.dir, err = os.MkdirTemp("", "pycompare-go-"); err != nil {
			return err
		}
		defer os.RemoveAll(c.dir)
	}
	for _, kind := range c.data {
		data, err := makeData(kind, c.shape)
		if err != nil {
			return err
		}
		if err := out.Encode(dataSet{Kind: "data", Lib: "go", Data: kind, CRC32: fmt.Sprintf("%08x", checksum(data))}); err != nil {
			return err
		}
		for _, store := range c.stores {
			for _, codec := range c.codecs {
				codecs, err := codecsOf(codec)
				if err != nil {
					return err
				}
				for r := 1; r <= c.runs; r++ {
					wr, rd, err := once(ctx, c, client, store, codecs, fmt.Sprintf("go-%s-%s-%d", kind, codec, r), data)
					if err != nil {
						return fmt.Errorf("%s data, %s, %s, run %d: %w", kind, store, codec, r, err)
					}
					for _, x := range []*result{&wr, &rd} {
						x.Kind, x.Lib, x.Data, x.Store, x.Codec, x.Run = "run", "go", kind, store, codec, r
						if err := out.Encode(x); err != nil {
							return err
						}
					}
					fmt.Fprintf(os.Stderr, "go %s %s %s run %d: write %.2f s, read %.2f s\n",
						kind, store, codec, r, wr.Seconds, rd.Seconds)
				}
			}
		}
	}
	return nil
}

// once writes the array, named name, into a store that holds nothing yet,
// reads it back, checks it and deletes it.
func once(ctx context.Context, c config, client *awss3.Client, store string, codecs []zarr.Codec, name string, data []float64) (wr, rd result, err error) {
	var (
		s     zarr.Store
		where string
	)
	switch store {
	case "memory":
		s = zarr.NewMemoryStore()
	case "dir":
		where = filepath.Join(c.dir, name+".zarr")
		s = zarr.NewDirStore(where)
		defer os.RemoveAll(where)
	case "s3":
		s = zarrs3.New(client, c.bucket, c.prefix+"/"+name)
		defer func() {
			if derr := zarr.Delete(ctx, s, ""); err == nil {
				err = derr
			}
		}()
	default:
		return wr, rd, fmt.Errorf("no store %q: memory, dir or s3", store)
	}

	start := make([]int, len(c.shape))
	runtime.GC()
	t := time.Now()
	a, err := zarr.CreateArray(ctx, s, "a", zarr.ArrayOptions{
		Shape: c.shape, ChunkShape: c.chunks, DataType: zarr.Float64, Codecs: codecs,
	})
	if err != nil {
		return wr, rd, err
	}
	a.Concurrency = c.concurrency
	if err := zarr.Write(ctx, a, start, c.shape, data); err != nil {
		return wr, rd, err
	}
	write := time.Since(t)

	runtime.GC()
	t = time.Now()
	a, err = zarr.OpenArray(ctx, s, "a")
	if err != nil {
		return wr, rd, err
	}
	a.Concurrency = c.concurrency
	got, err := zarr.Read[float64](ctx, a, start, c.shape)
	if err != nil {
		return wr, rd, err
	}
	read := time.Since(t)
	if !slices.Equal(got, data) {
		return wr, rd, errors.New("the array read is not the array written")
	}

	var stored int64
	if store == "dir" {
		if stored, err = du(where); err != nil {
			return wr, rd, err
		}
	}
	n := int64(len(data)) * 8
	wr = result{Op: "write", Seconds: write.Seconds(), Bytes: n, Stored: stored}
	rd = result{Op: "read", Seconds: read.Seconds(), Bytes: n, Stored: stored}
	return wr, rd, nil
}

// codecsOf is the pipeline of each codec name, as bench.py makes it for
// zarr-python: zstd at level 0, which is zstd's default and zarr-python 3's
// default compressor, and gzip at 5, GzipCodec's default there.
func codecsOf(name string) ([]zarr.Codec, error) {
	bytes := zarr.BytesCodec{Endian: zarr.Little}
	switch name {
	case "none":
		return []zarr.Codec{bytes}, nil
	case "zstd":
		return []zarr.Codec{bytes, zstd.Codec{Level: 0}}, nil
	case "gzip":
		return []zarr.Codec{bytes, zarr.GzipCodec{Level: 5}}, nil
	}
	return nil, fmt.Errorf("no codec %q: none, zstd or gzip", name)
}

// makeData is the array bench.py makes too, in C order. Each element is made
// from integers and at most one rounding, so both make the same bits and the
// codecs see the same bytes:
//
//   - random is a uniform number from 0 to 1 out of splitmix64 of i+1, for
//     element i. No codec compresses it.
//   - smooth is a tenth of a whole number that grows with the distance from
//     the middle of the first two dimensions and with the third, plus up to
//     25.5 of noise: a field measured to a tenth, which zstd and gzip store
//     in about a third of its size.
func makeData(kind string, shape []int) ([]float64, error) {
	out := make([]float64, shape[0]*shape[1]*shape[2])
	switch kind {
	case "random":
		for i := range out {
			out[i] = float64(mix(uint64(i)+1)>>11) * 0x1p-53
		}
	case "smooth":
		h0, h1 := shape[0]/2, shape[1]/2
		i := 0
		for x := range shape[0] {
			for y := range shape[1] {
				r := ((x-h0)*(x-h0) + (y-h1)*(y-h1)) >> 6
				for z := range shape[2] {
					q := r + z + int(mix(uint64(i)+1)&255)
					out[i] = float64(q) * 0.1
					i++
				}
			}
		}
	default:
		return nil, fmt.Errorf("no data %q: random or smooth", kind)
	}
	return out, nil
}

// mix is splitmix64's finaliser of x+golden ratio.
func mix(x uint64) uint64 {
	z := x * 0x9e3779b97f4a7c15
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb
	return z ^ z>>31
}

// checksum is the CRC-32 of the data's little-endian bytes, which bench.py
// prints too, to show both timed the same array.
func checksum(data []float64) uint32 {
	buf := make([]byte, 0, 1<<20)
	var crc uint32
	for _, v := range data {
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v))
		if len(buf) == cap(buf) {
			crc = crc32.Update(crc, crc32.IEEETable, buf)
			buf = buf[:0]
		}
	}
	return crc32.Update(crc, crc32.IEEETable, buf)
}

// version is the version of package zarr this was built with: the module's,
// or the commit of the repository it was built in.
func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	var rev, dirty string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "+dirty"
			}
		}
	}
	if rev != "" {
		return rev[:min(len(rev), 12)] + dirty
	}
	for _, d := range info.Deps {
		if d.Path == "github.com/LukasSelin/zarr" {
			return d.Version
		}
	}
	return ""
}

// du is how many bytes the files under dir take.
func du(dir string) (int64, error) {
	var n int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		n += info.Size()
		return nil
	})
	return n, err
}

func s3Client(ctx context.Context, c config) (*awss3.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if c.region != "" {
		opts = append(opts, awsconfig.WithRegion(c.region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		// MinIO and the like take any region, but a request must be signed
		// for one.
		cfg.Region = "us-east-1"
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if c.endpoint != "" {
			o.BaseEndpoint = aws.String(c.endpoint)
			o.UsePathStyle = true
		}
	}), nil
}
