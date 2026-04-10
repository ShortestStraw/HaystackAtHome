// All errors implement Is method, so that they can be checked with errors.Is()
// without matching error and target fields
package models

import "strconv"

type ErrUnimplemented struct {}

func (e *ErrUnimplemented) Error() string {
	return "Unimplememted"
}

type ErrInvalidParams struct {
	msg string
}

func NewErrInvalidParams(msg string) *ErrInvalidParams {
	return &ErrInvalidParams {
		msg: msg,
	}
}

func (e *ErrInvalidParams) Is(targer error) bool {
	_, ok := targer.(*ErrInvalidParams)
	return ok
}

func (e *ErrInvalidParams) Error() string {
	return e.msg
}

type ErrVolValidation struct {
	path string
	key  uint64  // Volume key
	msg  string
}

func NewErrVolValidation(msg string, key uint64, path string) *ErrVolValidation {
	return &ErrVolValidation{
		path: path,
		key: key,
		msg: msg,
	}
}

func (e *ErrVolValidation) Is(targer error) bool {
	_, ok := targer.(*ErrVolValidation)
	return ok
}

func (e *ErrVolValidation) Error() string {
	if e.key != 0 {
		e.msg = e.msg + ". Key: " + strconv.FormatUint(e.key, 10)
	}
	if e.path != "" {
		e.msg = e.msg + ". Path: " + e.path
	}
	return e.msg
}

func (e *ErrVolValidation) Key() uint64 {
	return e.key
}

func (e *ErrVolValidation) Path() string {
	return e.path
}

type ErrObjValidation struct {
	off   uint64 // The offset where validation failed
	msg   string
}

func NewErrObjValidation(msg string, off uint64) *ErrObjValidation {
	return &ErrObjValidation{
		off: off,
		msg: msg,
	}
}

func (e *ErrObjValidation) Error() string {
	if e.off != 0 {
		e.msg = e.msg + ". Offset: " + strconv.FormatUint(e.off, 10)
	}
	return e.msg
}

func (e *ErrObjValidation) Is(targer error) bool {
	_, ok := targer.(*ErrObjValidation)
	return ok
}

func (e *ErrObjValidation) Offset() uint64 {
	return e.off
}

type ErrObjCSMismatch struct {
	msg  string
}

func NewErrObjCSMismatch(msg string) *ErrObjCSMismatch {
	return &ErrObjCSMismatch{
		msg: msg,
	}
}

func (e *ErrObjCSMismatch) Is(targer error) bool {
	_, ok := targer.(*ErrObjCSMismatch)
	return ok
}

func (e *ErrObjCSMismatch) Error() string {
	return e.msg
}
