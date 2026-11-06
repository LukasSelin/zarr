package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

// TestData holds makeData to the checksums bench.py prints for the same
// shape, with numpy: if makeData changes and bench.py does not, the two time
// different bytes, and compare.py would set them side by side regardless.
func TestData(t *testing.T) {
	for kind, want := range map[string]uint32{"random": 0x1852347a, "smooth": 0x174fc653} {
		data, err := makeData(kind, []int{64, 64, 64})
		if err != nil {
			t.Fatal(err)
		}
		if got := checksum(data); got != want {
			t.Errorf("%s data: checksum %08x, bench.py makes %08x", kind, got, want)
		}
	}
}

// TestRun runs every store but s3 and every codec once, on a small array.
func TestRun(t *testing.T) {
	c := config{
		stores: []string{"memory", "dir"}, codecs: []string{"none", "zstd", "gzip"}, data: []string{"smooth"},
		runs: 1, dir: t.TempDir(), shape: []int{16, 16, 16}, chunks: []int{16, 16, 8},
	}
	var out bytes.Buffer
	if err := run(context.Background(), c, &out); err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	lines := bufio.NewScanner(&out)
	for lines.Scan() {
		var l result
		if err := json.Unmarshal(lines.Bytes(), &l); err != nil {
			t.Fatal(err)
		}
		kinds[l.Kind]++
		// A read of 32 KiB can take less than Windows' clock ticks: 0 s.
		if l.Kind == "run" && (l.Seconds < 0 || l.Bytes != 16*16*16*8 || l.Store == "dir" && l.Stored <= 0) {
			t.Errorf("%s", lines.Bytes())
		}
	}
	if kinds["meta"] != 1 || kinds["data"] != 1 || kinds["run"] != 2*3*2 {
		t.Errorf("lines of each kind: %v", kinds)
	}
}
