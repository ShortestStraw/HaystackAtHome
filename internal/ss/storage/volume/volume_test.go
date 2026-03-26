package volume

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"crypto/rand"
	"os"
	"testing"
	"HaystackAtHome/internal/ss/models"
)

func TestCreateAndOpen(t *testing.T) {
	logger_def := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))
	
	path := "./tescase/.volume.doesnotexist"
	logger := logger_def.With("volume", path)
	vol, err := Open(path, logger)
	if vol != nil {
		t.Errorf("vol = '%p', want 'nil'", vol)
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("err = '%s', want '%s'", err, os.ErrNotExist.Error())
		return
	}

	path = "testcase/.volume.1"
	logger = logger_def.With("volume", path)
	vol, err = CreateAndOpen(path, 0, 1024, logger)
	defer os.Remove(path)
	defer vol.Close()
	if vol == nil {
		t.Errorf("vol = 'nil', want nonil")
	}
	if err != nil {
		t.Errorf("err = '%s' want 'nil'", err.Error())
	}

	if vol.cursor.Load() != uint64(volumeHeaderOndiskSize) {
		t.Errorf("vol.cursor = '%d', want '%d'", vol.cursor.Load(), volumeHeaderOndiskSize)
	}

	if vol.sync_offset != vol.cursor.Load() {
		t.Errorf("vol.sync_offset '%d' != vol.cursor '%d'", vol.sync_offset, vol.cursor.Load())
	}
}

func TestIO(t *testing.T) {
	logger_def := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	path := "testcase/.volume.0"
	logger := logger_def.With("volume", path)
	vol, err := CreateAndOpen(path, 0, 1024 * 1024 * 1024, logger)
	if err != nil {
		t.Errorf("err = '%s' want 'nil'", err.Error())
	}
	defer os.Remove(path)
	defer vol.Close()

	{
		// normal read
		fd_rd := vol.Reader()

		buf := make([]byte, 40)
		n, err := fd_rd.ReadAt(buf, 0)
		if err != nil {
			t.Errorf("err = '%s', want = 'nil", err.Error())
		}
		if n != 40 {
			t.Errorf("n = '%d', want '40'", n)
		}

		// read until eof
		buf = make([]byte, 1024)
		n, err = fd_rd.ReadAt(buf, 0)
		if err != io.EOF {
			if err == nil {
				t.Errorf("err = 'nil', want '%s'", io.EOF.Error())
			} else {
				t.Errorf("err = '%s', want '%s'", err.Error(), io.EOF.Error())
			}
		}
		if n != 40 {
			t.Errorf("n = '%d', want '40'", n)
		}

		// read out of bounds
		n, err = fd_rd.ReadAt(buf, 1024)
		if err != io.EOF {
			if err == nil {
				t.Errorf("err = 'nil', want '%s'", io.EOF.Error())
			} else {
				t.Errorf("err = '%s', want '%s'", err.Error(), io.EOF.Error())
			}
		}
		if n != 0 {
			t.Errorf("n = '%d', want '0'", n)
		}
		fd_rd.Close()
	}
	
	{
		// normal write
		fd_wr := vol.Writer()

		var sz uint64 = 1024 * 1024
		buf := make([]byte, sz)
		buf_reader := bytes.NewReader(buf)

		written, err := io.Copy(fd_wr, buf_reader)
		if err != nil {
			t.Errorf("err = '%s', want 'nil'", err.Error())
		}
		if uint64(written) != sz {
			t.Errorf("written = '%d', want '%d'", written, sz)
		}
		if vol.cursor.Load() != vol.sync_offset + sz {
			t.Errorf("vol.cursor = '%d', want vol.sync_offset + %d = '%d'", vol.cursor.Load(), sz, vol.sync_offset + sz)
		}

		err = fd_wr.Sync()
		if vol.cursor.Load() != vol.sync_offset {
			t.Errorf("vol.cursor = '%d', want '%d' = vol.sync_offset", vol.cursor.Load(), vol.sync_offset)
		}
		fd_wr.Close()
	}
}

func TestReopen(t *testing.T) {
	logger_def := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	path := "testcase/.volume.2"
	logger := logger_def.With("volume", path)
	var maxSize uint64 = 1024 * 1024 * 1024
	vol, err := CreateAndOpen(path, 0, maxSize, logger)
	if err != nil {
		t.Errorf("err = '%s' want 'nil'", err.Error())
	}
	defer os.Remove(path)
	var sz uint64 = 1024 * 1024
	
	{
		fd_wr := vol.Writer()

		buf := make([]byte, sz)
		fd_wr.Write(buf)
		fd_wr.Close()
	}

	vol.Close() // implicitelly do Sync() if forgotten
	
	vol, err = Open(path, logger)
	defer vol.Close()

	if vol == nil {
		t.Errorf("vol = 'nil', want nonil")
	}
	if err != nil {
		t.Errorf("err = '%s' want 'nil'", err.Error())
	}
	if vol.cursor.Load() != uint64(volumeHeaderOndiskSize) + sz {
		t.Errorf("vol.cursor = '%d', want '%d'", vol.cursor.Load(), uint64(volumeHeaderOndiskSize) + sz)
	}
	if vol.sync_offset != vol.cursor.Load() {
		t.Errorf("vol.sync_offset '%d' != vol.cursor '%d'", vol.sync_offset, vol.cursor.Load())
	}
	header := VolumeHeader{
		Id: 0,
		MaxSize: maxSize,
		Version: currentVersion,
	}
	if vol.Header() != header {
		t.Errorf("vol.header = '%v', want '%v'", vol.header, header)
	}
}

func TestParallelIO(t *testing.T) {
	logger_def := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	path := "testcase/.volume.3"
	logger := logger_def.With("volume", path)
	var maxSize uint64 = 1024 * 1024 * 1024
	vol, err := CreateAndOpen(path, 0, maxSize, logger)
	if err != nil {
		t.Errorf("err = '%s' want 'nil'", err.Error())
	}
	defer os.Remove(path)
	defer vol.Close()

	wr_bufs := make([][]byte, 15)
	rd_bufs := make([][]byte, 15)
	for i := range wr_bufs {
		wr_bufs[i] = make([]byte, 1024 * 1024)
		n, err := rand.Read(wr_bufs[i])
		if err != nil {
			t.Errorf("err = '%s' want 'nil'", err.Error())
		}
		if n != 1024 * 1024 {
			t.Errorf("rand.Read read = '%d', want '%d'", n, 1024 * 1024)
		}

		rd_bufs[i] = make([]byte, 1024 * 1024)
	}

	fd_wr1 := vol.Writer()
	fd_wr2 := vol.Writer()
	writer := func (fd io.WriteCloser, bufs... []byte) bool {
		defer fd.Close()
		defer vol.Sync()
		for i, buf := range bufs {
			t.Log(i)
			buf_reader := bytes.NewReader(buf)
			written, err := io.Copy(fd, buf_reader)
			if err != nil {
				t.Logf("err = '%s'", err.Error())
				return false
			}
			if written != int64(len(buf)) {
				t.Logf("written = '%d' != '%d'", written, len(buf))
				return false
			}
		}
		return true
	}
	writer_test := func (fd io.WriteCloser, bufs... []byte) (func (t *testing.T)) {
		return func (t *testing.T) {
			t.Parallel()
			if !writer(fd, bufs...) {
				t.Fail()
			}
		}
	}
	
	fd_rewr := vol.Rewriter()

	fd_rd1 := vol.Reader()
	fd_rd2 := vol.Reader()
	fd_rd3 := vol.Reader()

	reader := func (fd models.ReadAtCloser, bufs... []byte) bool {
		defer fd.Close()
		for i, buf := range bufs {
			t.Log(i)
			_, err := fd.ReadAt(buf, int64(i * 1024 * 1024))
			if err != nil && err != io.EOF {
				t.Logf("err = '%s'", err.Error())
				return false
			}
		}
		return true
	}
	reader_test := func (fd models.ReadAtCloser, bufs... []byte) (func (t *testing.T)) {
		return func(t *testing.T) {
			t.Parallel()
			if !reader(fd, bufs...) {
				t.Fail()
			}
		}
	}
	t.Run("", func(t *testing.T) {
		t.Run("[writer 1]", writer_test(fd_wr1, wr_bufs[:8]...))
		t.Run("[writer 2]", writer_test(fd_wr2, wr_bufs[8:]...))
		t.Run("[rewriter]", func(t *testing.T) {
			t.Parallel()
			defer fd_rewr.Close()
			defer vol.Sync()
			buf := make([]byte, 1024 * 1024)
			for i := range 20 {
				t.Log(i)
				_, err := fd_rewr.WriteAt(buf, int64(i * 1024 * 1024))
				if err != nil && err != io.EOF {
					t.Logf("err = '%s'", err.Error())
				}
			}
		})
		t.Run("[reader 1]", reader_test(fd_rd1, rd_bufs[0:5]...))
		t.Run("[reader 2]", reader_test(fd_rd2, rd_bufs[5:10]...))
		t.Run("[reader 3]", reader_test(fd_rd3, rd_bufs[10:]...))
	})
}