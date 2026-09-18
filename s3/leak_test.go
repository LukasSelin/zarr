package s3

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package if a test left a goroutine running.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// net/http keeps a reader and a writer per pooled connection, and
		// they outlive the request that made them by design.
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}
