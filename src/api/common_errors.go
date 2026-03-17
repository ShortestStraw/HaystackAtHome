package api

import "errors"

var (
	ErrBadRequest   = errors.New("Request does not suit defined api")
	ErrSignMismatch = errors.New("Request signature mismatch")
)
