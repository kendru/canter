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
	"errors"
	"sync"
)

var (
	ErrNoSuchIdent        = errors.New("ident does not exist")
	ErrIdentAlreadyExists = errors.New("ident already exists")
)

var NullIdent = Ident{}

// Ident is an ident that has been resolved such that we can access both
// canonical ID and string representations.
type Ident struct {
	ID   ID
	Name string
}

func (ident Ident) String() string {
	return ident.Name
}

func (ident Ident) Resolve(conn *Connection) (ID, error) {
	if ident.ID != 0 {
		return ident.ID, nil
	}
	resolved, err := ResolveIdent(conn, ident.Name)
	if err != nil {
		return 0, err
	}
	return resolved.ID, nil
}

func (ident Ident) MustResolve(conn *Connection) ID {
	id, err := ident.Resolve(conn)
	if err != nil {
		panic(err)
	}
	return id
}

// IdentManager controls the mapping of string idents to IDs. Each peer
// maintains a cache of ident mappings in memory, which it hydrates on start-up.
// Because the system maintains a set of well-known idents < 0, the
// IdentManager must guarantee that it only allocates positive idents.
type IdentManager interface {
	// LoadIdents
	LoadIdents() ([]Ident, error)

	// LookupIdentIDs will return IDs for all of the names supplied. This will
	// return `ErrNoSuchIdent` if any name supplied does not represent a valid
	// ident.
	LookupIdentIDs([]string) ([]ID, error)

	// LookupIdentNames returns a list of names for the IDs supplied. This will
	// return `ErrNoSuchIdent` if any ID supplied does not represent a valid
	// ident.
	LookupIdentNames([]ID) ([]string, error)

	// StoreIdent will store the ident in the backing store. This will return
	// `ErrIdentAlreadyExists` if the ident already exists.
	StoreIdent(Ident) error
}

type identCache struct {
	mu           sync.Mutex
	idents       []Ident
	identIdxID   map[ID]int
	identIdxName map[string]int
}

func newIdentCache(mgr IdentManager) *identCache {
	c := &identCache{
		idents:       make([]Ident, 0, 256),
		identIdxID:   make(map[ID]int, 256),
		identIdxName: make(map[string]int, 256),
	}

	c.store([]Ident{
		{
			ID:   IDID,
			Name: "db/id",
		},
		{
			ID:   IDIdent,
			Name: "db/ident",
		},
		{
			ID:   IDType,
			Name: "db/type",
		},
		{
			ID:   IDCompositeComponents,
			Name: "db/compositeComponents",
		},
		{
			ID:   IDCardinality,
			Name: "db/cardinality",
		},
		{
			ID:   IDUnique,
			Name: "db/unique",
		},
		{
			ID:   IDIndexed,
			Name: "db/indexed",
		},
		{
			ID:   IDDoc,
			Name: "db/doc",
		},
		{
			ID:   IDTxCommitTime,
			Name: "db.tx/commitTime",
		},
		{
			ID:   IDCardinalityOne,
			Name: "db.cardinality/one",
		},
		{
			ID:   IDCardinalityMany,
			Name: "db.cardinality/many",
		},
		{
			ID:   IDTypeString,
			Name: "db.type/string",
		},
		{
			ID:   IDTypeBoolean,
			Name: "db.type/boolean",
		},
		{
			ID:   IDTypeInt64,
			Name: "db.type/int64",
		},
		{
			ID:   IDTypeInt32,
			Name: "db.type/int32",
		},
		{
			ID:   IDTypeInt16,
			Name: "db.type/int16",
		},
		{
			ID:   IDTypeInt8,
			Name: "db.type/int8",
		},
		{
			ID:   IDTypeFloat64,
			Name: "db.type/float64",
		},
		{
			ID:   IDTypeFloat32,
			Name: "db.type/float32",
		},
		{
			ID:   IDTypeDecimal,
			Name: "db.type/decimal",
		},
		{
			ID:   IDTypeTimestamp,
			Name: "db.type/timestamp",
		},
		{
			ID:   IDTypeDate,
			Name: "db.type/date",
		},
		{
			ID:   IDTypeRef,
			Name: "db.type/ref",
		},
		{
			ID:   IDTypeBinary,
			Name: "db.type/binary",
		},
		{
			ID:   IDTypeUUID,
			Name: "db.type/uuid",
		},
		{
			ID:   IDTypeULID,
			Name: "db.type/ulid",
		},
		{
			ID:   IDTypeComposite,
			Name: "db.type/composite",
		},
	})

	return c
}

func (c *identCache) store(idents []Ident) {
	c.mu.Lock()
	defer c.mu.Unlock()

	idx := len(c.idents)
	for _, ident := range idents {
		if _, found := c.identIdxID[ident.ID]; found {
			return
		}
		c.idents = append(c.idents, ident)
		c.identIdxID[ident.ID] = idx
		c.identIdxName[ident.Name] = idx
		idx += 1
	}
}

func (c *identCache) lookupByID(id ID) (Ident, bool) {
	if idx, ok := c.identIdxID[id]; ok {
		return c.idents[idx], true
	}
	return NullIdent, false
}

func (c *identCache) lookupByName(name string) (Ident, bool) {
	if idx, ok := c.identIdxName[name]; ok {
		return c.idents[idx], true
	}
	return NullIdent, false
}
