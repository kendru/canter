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

import "github.com/oklog/ulid/v2"

type IDManager interface {
	NextID() (ID, error)
}

// Marker interface for IDs and things that can be resolved to IDs at
// transaction time.
type identifier interface {
	identify()
}

// ID is an identifier that uniquely identifies an entity or ident.
//
//go:generate stringer -type ID -trimprefix ID
type ID int64

func (id ID) identify() {}

func (id ID) Resolve(conn *Connection) (ID, error) {
	return id, nil
}

// tempID is a placeholder that may be repeated within a transaction and will be
// replaced by the same ID everywhere it occurs.
type tempID struct {
	symbol string
}

func (id tempID) identify() {}

func TempID() tempID {
	return tempID{
		symbol: ulid.Make().String(),
	}
}

const (
	// System-managed idents.
	IDID ID = -1*iota - 1
	IDIdent
	IDType
	IDCompositeComponents
	IDCardinality
	IDUnique
	IDIndexed
	IDDoc
	IDTxCommitTime

	// System-managed enumerated values.
	IDCardinalityOne
	IDCardinalityMany

	IDTypeString ID = -500 + -1*iota
	IDTypeBoolean
	IDTypeInt64
	IDTypeInt32
	IDTypeInt16
	IDTypeInt8
	IDTypeFloat64
	IDTypeFloat32
	IDTypeDecimal
	IDTypeTimestamp
	IDTypeDate
	IDTypeRef
	IDTypeBinary
	IDTypeUUID
	IDTypeULID
	IDTypeComposite
)
