package badger

import (
	"fmt"

	"github.com/kendru/canter/internal/store"
)

func (sto *badgerStore) NextID() (store.ID, error) {
	id, err := sto.idSeq.Next()
	if err != nil {
		return store.ID(0), fmt.Errorf("allocating new ID: %w", err)
	}
	return store.ID(id), nil
}
