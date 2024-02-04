// Copyright 2024 Andrew Meredith
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
