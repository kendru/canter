package badger

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

const (
	tblPrefixIdents byte = iota
	tblPrefixIdentIDByName
	tblPrefixEAVT
	tblPrefixAEVT
	tblPrefixAVET
	tblPrefixVAET
	seqID
)

const seqIDPrefetchCount uint64 = 100

func New(db *badger.DB) (*badgerStore, error) {
	idSeq, err := db.GetSequence([]byte{seqID}, seqIDPrefetchCount)
	if err != nil {
		return nil, fmt.Errorf("getting sequence for IDs: %w", err)
	}

	return &badgerStore{
		db:    db,
		idSeq: idSeq,
	}, nil
}

type badgerStore struct {
	db    *badger.DB
	idSeq *badger.Sequence
}
