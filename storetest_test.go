package zarr_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/LukasSelin/zarr"
	"github.com/LukasSelin/zarr/storetest"
)

func TestTheStoresKeepTheStoreContract(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		storetest.Run(t, func() zarr.Store { return zarr.NewMemoryStore() })
	})
	t.Run("directory", func(t *testing.T) {
		storetest.Run(t, func() zarr.Store { return zarr.NewDirStore(t.TempDir()) })
	})
	t.Run("memory, read only", func(t *testing.T) {
		storetest.RunReadOnly(t, func(seed map[string][]byte) zarr.Store {
			m := zarr.NewMemoryStore()
			for k, v := range seed {
				m.Set(context.Background(), k, v)
			}
			return readOnly{m}
		})
	})
}

// readOnly is a MemoryStore that refuses to be written but lists.
type readOnly struct{ *zarr.MemoryStore }

func (readOnly) Set(_ context.Context, key string, _ []byte) error {
	return fmt.Errorf("%w: %s", zarr.ErrReadOnly, key)
}

func (readOnly) Delete(_ context.Context, key string) error {
	return fmt.Errorf("%w: %s", zarr.ErrReadOnly, key)
}
