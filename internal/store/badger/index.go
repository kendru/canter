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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/kendru/canter/internal/store"
	"github.com/kendru/canter/pkg/dataflow"
)

func (sto *badgerStore) Write(assertions []store.ResolvedAssertion) error {
	return sto.db.Update(func(txn *badger.Txn) error {
		// TODO: Write transaction entity data.

		for _, assertion := range assertions {
			// Write to EAVT
			if err := writeEAVT(txn, assertion); err != nil {
				return err
			}
			if err := writeAVET(txn, assertion); err != nil {
				return err
			}
			// TODO: Write to other indexes.
		}
		return nil
	})
}

func (sto *badgerStore) ScanEAVT(entityID store.ID, attribute *store.ID) (dataflow.Producer[store.Fact], error) {
	prefix := []byte{tblPrefixEAVT}
	prefix = binary.BigEndian.AppendUint64(prefix, uint64(entityID))
	if attribute != nil {
		prefix = binary.BigEndian.AppendUint64(prefix, uint64(*attribute))
	}

	var facts []store.Fact
	if err := sto.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			fct := store.Fact{
				EntityID: entityID,
			}
			if attribute == nil {
				key := it.Item().Key()
				fct.Attribute = store.ID(binary.BigEndian.Uint64(key[9:]))
			} else {
				fct.Attribute = *attribute
			}

			if err := it.Item().Value(func(val []byte) error {
				// XXX: Determine what to do with removed/superseded facts.
				assertMode := store.AssertMode(val[0])
				if assertMode != store.AssertModeAddition {
					return nil
				}

				fct.Tx = store.ID(binary.BigEndian.Uint64(val[1:]))

				dec := gob.NewDecoder(bytes.NewReader(val[9:]))
				// We could either encode a type in the value, or we could look
				// up the attribute's type in the schema. This would require us
				// to look up the schema on a "smart path" that does not rely on
				// ScanEAVT itself. We could also cache the schema in the store,
				// assuming that the type of an attribute is immutable or we
				// have a way to invalidate the cache.
				// If we store a type tag in the value, we could support schema
				// evolution by deferring rewriting the value until it is read.
				attrType, err := sto.typeFor(fct.Attribute)
				if err != nil {
					return err
				}
				switch attrType {
				case store.IDTypeRef:
					var ref store.ID
					if err := dec.Decode(&ref); err != nil {
						return fmt.Errorf("decoding ref value: %w", err)
					}
					fct.Value = store.Value(ref)
				case store.IDTypeString:
					var str string
					if err := dec.Decode(&str); err != nil {
						return fmt.Errorf("decoding string value: %w", err)
					}
					fct.Value = store.Value(str)
				case store.IDTypeInt64:
					var i int64
					if err := dec.Decode(&i); err != nil {
						return fmt.Errorf("decoding int64 value: %w", err)
					}
					fct.Value = store.Value(i)
				case store.IDTypeFloat64:
					var f float64
					if err := dec.Decode(&f); err != nil {
						return fmt.Errorf("decoding float64 value: %w", err)
					}
					fct.Value = store.Value(f)
				case store.IDTypeBoolean:
					var b bool
					if err := dec.Decode(&b); err != nil {
						return fmt.Errorf("decoding bool value: %w", err)
					}
					fct.Value = store.Value(b)
				case store.IDTypeBinary:
					fct.Value = store.Value(val[9:])
				default:
					return fmt.Errorf("unsupported value type for attribute %q: %q", fct.Attribute, attrType)
				}

				return nil
			}); err != nil {
				return err
			}

			facts = append(facts, fct)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return dataflow.SliceScanner[store.Fact]{Slice: facts}, nil
}

func (sto *badgerStore) ScanAEVT(attribute store.ID, entityID *store.ID) (dataflow.Producer[store.Fact], error) {
	panic("badgerStore.ScanAEVT() not yet implemented.")
}

func (sto *badgerStore) ScanAVET(attribute store.ID, val store.Value) (dataflow.Producer[store.Fact], error) {
	if val == nil {
		return nil, fmt.Errorf("nil value not supported")
	}

	prefix := []byte{tblPrefixAVET}
	prefix = binary.BigEndian.AppendUint64(prefix, uint64(attribute))
	// See NOTE [VALUE-ENCODING].
	prefixBuf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(prefixBuf).Encode(val); err != nil {
		return nil, fmt.Errorf("encoding value: %w", err)
	}

	var facts []store.Fact
	if err := sto.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			fct := store.Fact{
				Attribute: attribute,
				Value:     val,
			}

			if err := it.Item().Value(func(val []byte) error {
				// XXX: Determine what to do with removed/superseded facts.
				assertMode := store.AssertMode(val[0])
				if assertMode != store.AssertModeAddition {
					return nil
				}

				fct.Tx = store.ID(binary.BigEndian.Uint64(val[1:]))
				fct.EntityID = store.ID(binary.BigEndian.Uint64(val[9:]))
				return nil
			}); err != nil {
				return err
			}
			facts = append(facts, fct)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return dataflow.SliceScanner[store.Fact]{Slice: facts}, nil
}

func (sto *badgerStore) ScanVAET(val store.Value, attribute *store.ID) (dataflow.Producer[store.Fact], error) {
	panic("badgerStore.ScanVAET() not yet implemented.")
}

func writeEAVT(txn *badger.Txn, assertion store.ResolvedAssertion) error {
	// DEBUG
	// fmt.Printf("writing EAVT assertion: %v\n", assertion)
	// fmt.Printf("\t[%d, %d, %v, %d, %s]\n", assertion.EntityID, assertion.Attribute, assertion.Value, assertion.Tx, assertion.Mode())

	key := make([]byte, 17)
	key[0] = tblPrefixEAVT
	binary.BigEndian.PutUint64(key[1:], uint64(assertion.EntityID))
	binary.BigEndian.PutUint64(key[9:], uint64(assertion.Attribute))

	val := make([]byte, 9)
	val[0] = uint8(assertion.Mode())

	binary.BigEndian.PutUint64(val[1:], uint64(assertion.Tx))
	// NOTE [VALUE-ENCODING]:
	// We are currently encoding the value as a gob. This is not ideal, as
	// we cannot guarantee that the gob encoding will be stable across
	// versions of the code or that values will be ordered correctly.
	// We may not need to ensure ordering, but if we do, we should consider
	// using an encoding scheme like FoundationDB's Tuple encoding.
	valBuf := bytes.NewBuffer(val)
	if err := gob.NewEncoder(valBuf).Encode(assertion.Value); err != nil {
		return fmt.Errorf("encoding value: %w", err)
	}
	// assertion.Value.EncodeAs(assertion.Attribute, valBuf)

	return txn.Set(key, valBuf.Bytes())
}

func writeAVET(txn *badger.Txn, assertion store.ResolvedAssertion) error {
	// Since the value contains the key, allocate a reasonable amount of
	// space for the key.
	key := make([]byte, 9, 64)
	key[0] = tblPrefixAVET
	binary.BigEndian.PutUint64(key[1:], uint64(assertion.Attribute))
	// See NOTE [VALUE-ENCODING].
	keyBuf := bytes.NewBuffer(key)
	if err := gob.NewEncoder(keyBuf).Encode(assertion.Value); err != nil {
		return fmt.Errorf("encoding value: %w", err)
	}

	val := make([]byte, 17)
	val[0] = uint8(assertion.Mode())
	binary.BigEndian.PutUint64(val[1:], uint64(assertion.Tx))
	binary.BigEndian.PutUint64(val[9:], uint64(assertion.EntityID))

	return txn.Set(keyBuf.Bytes(), val)
}

// typeFor returns the type of the attribute.
// TODO: Cache values.
func (sto *badgerStore) typeFor(attribute store.ID) (attrTypeID store.ID, err error) {
	typeID := int64(store.IDType)
	key := make([]byte, 17)
	key[0] = tblPrefixEAVT
	binary.BigEndian.PutUint64(key[1:], uint64(attribute))
	binary.BigEndian.PutUint64(key[9:], uint64(typeID))

	err = sto.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			// Skip mode bit + tx id.
			data := bytes.NewReader(val[9:])
			// See NOTE [VALUE-ENCODING].
			dec := gob.NewDecoder(data)
			return dec.Decode(&attrTypeID)
		})
	})

	if err != nil {
		err = fmt.Errorf("fetching type for attribute %q: %w", attribute, err)
	}

	return
}
