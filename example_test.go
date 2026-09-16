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
