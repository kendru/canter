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

package store

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kendru/canter/pkg/rtype"
)

type TypeTag uint8

const (
	TypeTagString TypeTag = iota
	TypeTagPosInt64
	TypeTagNegInt64
	TypeTagFloat64
	TypeTagBool
	TypeTagUUID
	TypeTagTuple
)

// Value is an untyped value. Typically it is used when type
// information is stored separately from the value (e.g. in a
// TupleHeader).
type Value any

// EncodedValue is a value that has been encoded into a byte slice.
type EncodedValue []byte

// Value is a typed value that can be stored in a database.
type TypedValue struct {
	Type  rtype.ConcreteType
	Value any
}

func (v TypedValue) Encode(w io.Writer) error {
	switch rtype.RootType(v.Type) {
	case rtype.RTypeString:
		panic("TODO: Encode string")

	case rtype.RTypeInt64:
		out := make([]byte, 9)

		n := v.Value.(int64)
		if n >= 0 {
			out[0] = byte(TypeTagPosInt64)
			binary.BigEndian.PutUint64(out[1:], uint64(n))
		} else {
			// Encode as one's complement.
			out[0] = byte(TypeTagNegInt64)
			n *= -1
			for i := 0; i < 8; i++ {
				out[i+1] = 0xff
			}
		}
		if _, err := w.Write(out); err != nil {
			return fmt.Errorf("writing int64: %w", err)
		}

	default:
		panic(fmt.Sprintf("unsupported type: %v", v.Type))
	}

	return nil
}

// Tuple is a tuple of values that are encoded together.
// They are typically used as keys in the database's indexes.
// One advantage of tuple encoding is that it allows for
// efficient prefix scans, as the tuple is encoded in a
// lexicographically sortable way.
// Our tuples are inspired by FoundationDB's Tuple layer.
// See https://apple.github.io/foundationdb/data-modeling.html#tuples
// and https://github.com/apple/foundationdb/blob/main/design/tuple.md
// for more information.
type EncodedTuple []byte

type TypedTuple struct {
	TupleHeader
	Values []Value
}

type TupleHeader struct {
	Types []rtype.ConcreteType
}

func NewTypedTuple(values ...TypedValue) TypedTuple {
	types := make([]rtype.ConcreteType, len(values))
	vals := make([]Value, len(values))
	for i, v := range values {
		types[i] = v.Type
		vals[i] = v.Value
	}

	return TypedTuple{
		TupleHeader: TupleHeader{Types: types},
		Values:      vals,
	}
}
