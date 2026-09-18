package zarr_test

import (
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
}
