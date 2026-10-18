package geodb

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestAWindowIsThePixelsWhoseCentresAreInTheBox(t *testing.T) {
	g := Grid{CRS: "EPSG:4326", X0: 10, Y0: 60, Dx: 1, Dy: 1, Width: 10, Height: 5}
	for _, c := range []struct {
		b    BBox
		want Window
		ok   bool
	}{
		{BBox{10, 55, 20, 60}, Window{0, 0, 10, 5}, true},
		{BBox{10.5, 59.5, 10.5, 59.5}, Window{0, 0, 1, 1}, true}, // a centre, exactly
		{BBox{10.6, 59, 11.4, 59.4}, Window{}, false},            // between centres
		{BBox{0, 0, 100, 100}, Window{0, 0, 10, 5}, true},        // clipped
		{BBox{12.2, 56.1, 14.9, 58.8}, Window{2, 1, 3, 3}, true},
		{BBox{30, 55, 40, 60}, Window{}, false},
		{BBox{math.NaN(), 55, 12, 60}, Window{}, false},
	} {
		got, ok := g.Window(c.b)
		if ok != c.ok || got != c.want {
			t.Errorf("Window(%v) = %v, %v; want %v, %v", c.b, got, ok, c.want, c.ok)
		}
	}
}

func TestAPixelIsTheOneAPointIsIn(t *testing.T) {
	g := Grid{CRS: "EPSG:32633", X0: 500000, Y0: 6200000, Dx: 10, Dy: 10, Width: 100, Height: 50}
	for _, c := range []struct {
		p        Point
		col, row int
		ok       bool
	}{
		{Point{500000, 6200000}, 0, 0, true},
		{Point{500009.9, 6199990.1}, 0, 0, true},
		{Point{500010, 6199990}, 1, 1, true},
		{Point{500999.9, 6199500.1}, 99, 49, true},
		{Point{501000, 6199600}, 0, 0, false},
		{Point{499999, 6199600}, 0, 0, false},
		{Point{500500, 6200001}, 0, 0, false},
	} {
		col, row, ok := g.Pixel(c.p)
		if ok != c.ok || ok && (col != c.col || row != c.row) {
			t.Errorf("Pixel(%v) = %d, %d, %v; want %d, %d, %v", c.p, col, row, ok, c.col, c.row, c.ok)
		}
	}
	if p := g.Center(3, 2); p != (Point{500035, 6199975}) {
		t.Errorf("Center(3, 2) = %v", p)
	}
}

func TestEvenOddFillsAHoleAndTwoParts(t *testing.T) {
	outer := []Point{{0, 0}, {10, 0}, {10, 10}, {0, 10}}
	hole := []Point{{3, 3}, {7, 3}, {7, 7}, {3, 7}}
	apart := []Point{{20, 0}, {22, 0}, {22, 2}, {20, 2}, {20, 0}} // closed explicitly
	rings := [][]Point{outer, hole, apart}
	for _, c := range []struct {
		p    Point
		want bool
	}{
		{Point{1, 1}, true}, {Point{5, 5}, false}, {Point{8, 5}, true},
		{Point{21, 1}, true}, {Point{15, 1}, false}, {Point{-1, 5}, false},
	} {
		if got := inside(rings, c.p); got != c.want {
			t.Errorf("inside(%v) = %v", c.p, got)
		}
	}
}

// bruteForce is the pixels whose centres are inside rings, one at a time.
func bruteForce(g Grid, rings [][]Point) map[[2]int]bool {
	out := map[[2]int]bool{}
	for row := range g.Height {
		for col := range g.Width {
			if inside(rings, g.Center(col, row)) {
				out[[2]int{col, row}] = true
			}
		}
	}
	return out
}

func spanPixels(ss []span) map[[2]int]bool {
	out := map[[2]int]bool{}
	for _, s := range ss {
		for c := s.c0; c < s.c1; c++ {
			out[[2]int{int(c), int(s.row)}] = true
		}
	}
	return out
}

func TestRasterizingIsWhatTestingEveryPixelGives(t *testing.T) {
	g := Grid{CRS: "EPSG:4326", X0: 0, Y0: 40, Dx: 1, Dy: 1, Width: 40, Height: 40}
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 200 {
		// A star-shaped ring around a centre, sometimes with a hole, and
		// sometimes running off the grid.
		cx, cy := r.Float64()*50-5, r.Float64()*50-5
		var rings [][]Point
		for k, radius := range []float64{2 + r.Float64()*12, 1 + r.Float64()*2} {
			if k == 1 && i%2 == 0 {
				break
			}
			n := 3 + r.IntN(12)
			ring := make([]Point, n)
			for j := range n {
				a := 2 * math.Pi * float64(j) / float64(n)
				d := radius * (0.5 + r.Float64()/2)
				ring[j] = Point{cx + d*math.Cos(a), cy + d*math.Sin(a)}
			}
			rings = append(rings, ring)
		}
		want := bruteForce(g, rings)
		got := spanPixels(rasterize(g, rings))
		if len(want) == 0 {
			if len(got) > 1 {
				t.Fatalf("parcel %d holds no centre and was rasterized to %d pixels", i, len(got))
			}
			continue
		}
		if len(got) != len(want) {
			t.Fatalf("parcel %d: %d pixels, want %d", i, len(got), len(want))
		}
		for p := range want {
			if !got[p] {
				t.Fatalf("parcel %d: pixel %v is missing", i, p)
			}
		}
	}
}

func TestAParcelSmallerThanAPixelIsThePixelItIsIn(t *testing.T) {
	g := Grid{CRS: "EPSG:4326", X0: 0, Y0: 10, Dx: 1, Dy: 1, Width: 10, Height: 10}
	ss := rasterize(g, [][]Point{{{3.1, 6.1}, {3.3, 6.1}, {3.3, 6.3}}})
	if want := []span{{3, 3, 4}}; !slices.Equal(ss, want) {
		t.Errorf("spans = %v, want %v", ss, want)
	}
	if ss := rasterize(g, [][]Point{{{30, 6}, {31, 6}, {31, 7}}}); ss != nil {
		t.Errorf("a parcel off the grid has the spans %v", ss)
	}
}

func TestHilbertVisitsEveryCellOnce(t *testing.T) {
	// On a grid of 4 by 4 at the top of the curve's order, the distances
	// of the 16 cells, scaled, are 16 distinct steps.
	seen := map[uint64]bool{}
	for x := range uint32(4) {
		for y := range uint32(4) {
			seen[hilbert(x<<14, y<<14)>>28] = true
		}
	}
	if len(seen) != 16 {
		t.Fatalf("%d distinct distances, want 16", len(seen))
	}
	// Neighbours along the curve are neighbours on the grid.
	pos := map[uint64][2]int{}
	for x := range uint32(8) {
		for y := range uint32(8) {
			pos[hilbert(x<<13, y<<13)>>26] = [2]int{int(x), int(y)}
		}
	}
	for d := range uint64(63) {
		a, b := pos[d], pos[d+1]
		if abs(a[0]-b[0])+abs(a[1]-b[1]) != 1 {
			t.Fatalf("steps %d and %d of the curve are %v and %v", d, d+1, a, b)
		}
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func TestTheRTreeFindsWhatAScanFinds(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for _, n := range []int{0, 1, 15, 16, 17, 300, 5000} {
		boxes := make([]BBox, n)
		for i := range boxes {
			x, y := r.Float64()*100, r.Float64()*100
			boxes[i] = BBox{x, y, x + r.Float64()*3, y + r.Float64()*3}
		}
		tree := newRTree(boxes)
		for range 50 {
			x, y := r.Float64()*100, r.Float64()*100
			q := BBox{x, y, x + r.Float64()*20, y + r.Float64()*20}
			var got, want []int
			tree.search(q, func(i int) { got = append(got, i) })
			for i, b := range boxes {
				if b.Intersects(q) {
					want = append(want, i)
				}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("n=%d: search(%v) = %v, want %v", n, q, got, want)
			}
		}
	}
}
