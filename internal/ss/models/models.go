package models

import "io"

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type WriteAtCloser interface {
	io.WriterAt
	io.Closer
}