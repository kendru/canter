/*
Copyright 2024 Andrew Meredith

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/dgraph-io/badger/v4"
	"github.com/kendru/canter/internal/store"
)

func (s *badgerStore) LoadIdents() ([]store.Ident, error) {
	var idents []store.Ident
	opts := badger.IteratorOptions{
		Prefix: []byte{tblPrefixIdents},
	}
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte{tblPrefixIdents}); it.Valid(); it.Next() {
			// Key layout:
			// | table prefix |    id   | name |
			// |   1 byte     | 8 bytes | ...  |

			// Note that the ident table acts as a sorted set, and the
			// associated values are empty.
			key := it.Item().Key()
			id, err := strconv.ParseInt(string(key[1:9]), 10, 64)
			if err != nil {
				panic("malformed id. database corrupt.")
			}
			name := make([]byte, 0, len(key)-9)
			key = append(name, key[9:]...)
			idents = append(idents, store.Ident{
				ID:   store.ID(id),
				Name: string(name),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return idents, nil
}

func (s *badgerStore) LookupIdentIDs(names []string) ([]store.ID, error) {
	ids := make([]store.ID, len(names))

	key := make([]byte, 0, 64)
	err := s.db.View(func(txn *badger.Txn) error {
		for idx, name := range names {
			key = append(key[:0], tblPrefixIdentIDByName)
			key = append(key, name...)
			item, err := txn.Get(key)
			switch err {
			case nil:
				// Found ID for name - decode ID and append to output.
				if err := item.Value(func(val []byte) error {
					ids[idx] = store.ID(binary.BigEndian.Uint64(val))
					return nil
				}); err != nil {
					return fmt.Errorf("getting ID for name: %w", err)
				}

			case badger.ErrKeyNotFound:
				return errors.Join(
					fmt.Errorf("no ident for name %q", name),
					store.ErrNoSuchIdent,
				)
			default:
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return ids, nil
}

func (s *badgerStore) LookupIdentNames(ids []store.ID) ([]string, error) {
	names := make([]string, len(ids))

	err := s.db.View(func(txn *badger.Txn) error {
		// Use same memory for prefix each iteration
		keyPrefix := make([]byte, 1+8)
		keyPrefix[0] = tblPrefixIdents
		it := txn.NewIterator(badger.IteratorOptions{})
		defer it.Close()
		for idx, id := range ids {
			binary.BigEndian.PutUint64(keyPrefix[1:], uint64(id))
			it.Seek(keyPrefix)
			if !it.ValidForPrefix(keyPrefix) {
				return errors.Join(
					fmt.Errorf("no ident for id %d", id),
					store.ErrNoSuchIdent,
				)
			}
			key := it.Item().Key()
			names[idx] = string(append([]byte{}, key[9:]...))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return names, err
}

func (s *badgerStore) StoreIdent(ident store.Ident) error {
	id := ident.ID
	name := ident.Name

	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], uint64(id))

	// Table prefix: 1 byte
	// ID:           8 bytes
	// Name:         variable length
	identsKey := make([]byte, 1+8+len(name))
	identsKey[0] = tblPrefixIdents
	copy(identsKey[1:], idBytes[:])
	copy(identsKey[9:], name)

	identIDByNameKey := append([]byte{tblPrefixIdentIDByName}, name...)

	return s.db.Update(func(txn *badger.Txn) error {
		// Set entry in Idents table.
		if err := txn.Set(identsKey, nil); err != nil {
			return err
		}

		// Set entry in IdentIDByName table.
		if err := txn.Set(identIDByNameKey, idBytes[:]); err != nil {
			return err
		}

		return nil
	})
}
