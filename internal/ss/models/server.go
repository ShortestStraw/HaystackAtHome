package models

import "context"

type Server interface {
	ListenAndServe(ctx context.Context) error
}