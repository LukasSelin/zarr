package geodb

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if a test left a goroutine running: composites
// are made a block per worker, and the cache has reads waiting on others.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
