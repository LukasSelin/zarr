package zstd

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if a test left a goroutine running: the zstd
// encoder and decoder start workers of their own, so a Close that was missed
// shows up here.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
