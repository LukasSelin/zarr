package zarr_test

import (
	"context"
	"fmt"

	"github.com/LukasSelin/zarr"
)

func Example() {
	ctx := context.Background()
	s := zarr.NewMemoryStore() // or zarr.NewDirStore("world.zarr")

	root, err := zarr.CreateGroup(ctx, s, "", map[string]any{"seed": 7})
	if err != nil {
		panic(err)
	}
	height, err := root.CreateArray(ctx, "height", zarr.ArrayOptions{
		Shape:          []int{4, 6},
		ChunkShape:     []int{2, 4},
		DataType:       zarr.Float32,
		Codecs:         []zarr.Codec{zarr.BytesCodec{Endian: zarr.Little}, zarr.GzipCodec{Level: 5}},
		DimensionNames: []string{"y", "x"},
	})
	if err != nil {
		panic(err)
	}

	row := []float32{10, 11, 12, 13, 14, 15}
	if err := zarr.Write(ctx, height, []int{2, 0}, []int{1, 6}, row); err != nil {
		panic(err)
	}
	got, err := zarr.Read[float32](ctx, height, []int{1, 3}, []int{2, 2})
	if err != nil {
		panic(err)
	}
	fmt.Println(got)
	fmt.Println(s.Keys())
	// Output:
	// [0 0 13 14]
	// [height/c/1/0 height/c/1/1 height/zarr.json zarr.json]
}

// Delete removes a node and everything under it: the whole of an array that
// is no longer wanted, whatever keys its chunks are under.
func ExampleDelete() {
	ctx := context.Background()
	s := zarr.NewMemoryStore()

	root, err := zarr.CreateGroup(ctx, s, "", nil)
	if err != nil {
		panic(err)
	}
	for _, name := range []string{"height", "depth"} {
		a, err := root.CreateArray(ctx, name, zarr.ArrayOptions{
			Shape: []int{4}, ChunkShape: []int{2}, DataType: zarr.Int32,
		})
		if err != nil {
			panic(err)
		}
		if err := zarr.Write(ctx, a, []int{0}, []int{4}, []int32{1, 2, 3, 4}); err != nil {
			panic(err)
		}
	}
	children, err := root.Children(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(children)

	if err := zarr.Delete(ctx, s, "depth"); err != nil {
		panic(err)
	}
	fmt.Println(s.Keys())
	// Output:
	// [{depth array} {height array}]
	// [height/c/0 height/c/1 height/zarr.json zarr.json]
}
