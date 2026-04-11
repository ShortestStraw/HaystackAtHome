package volume

import (
	"io"
	"log/slog"
	"os"
)

func testLogger() *slog.Logger {
	if os.Getenv("TEST_LOG") == "1" {
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
