package zarr

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if a test left a goroutine running. Read, Write
// and Resize start a worker per stored object and must gather every one of
// them back before they return, whether the work finished, a chunk failed to
// decode, or the caller's context was cancelled; a goroutine still alive at
// the end of the run is that contract broken.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
