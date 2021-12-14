package work

import (
	"go.uber.org/zap"
)

func newNopLogger() *zap.Logger {
	return zap.NewNop()
}

func ensureLogger(t *Team) {
	if t.Logger == nil {
		t.Logger = newNopLogger()
	}
}
