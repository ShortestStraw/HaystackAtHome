package storage

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Mkdir("testcase", 0o775)
	if os.Getenv("TEST_LOG") != "1" {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	if err := generateFixture(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL\tsetup fixture: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
