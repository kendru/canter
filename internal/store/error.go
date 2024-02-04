package store

import "fmt"

var (
	ErrNoSuchEntity = fmt.Errorf("no such entity")
	ErrConflict     = fmt.Errorf("conflict")
)
