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

package store_test

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/kendru/canter/internal/store"
	badgerImpl "github.com/kendru/canter/internal/store/badger"
	"github.com/stretchr/testify/assert"
)

func TestResolveSystemIdents(t *testing.T) {
	t.SkipNow()
	conn := newMemoryConnection()

	sysIdents, err := conn.ResolveIdents([]any{
		"db/ident",
		"db/type",
		"db/cardinality",
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []store.Ident{
		{ID: store.IDIdent, Name: "db/ident"},
		{ID: store.IDType, Name: "db/type"},
		{ID: store.IDCardinality, Name: "db/cardinality"},
	}, sysIdents)

	sysIdentsByID, err := conn.ResolveIdents([]any{
		store.IDIdent,
		store.IDType,
		store.IDCardinality,
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, sysIdents, sysIdentsByID)
}

func TestResolveUserIdents(t *testing.T) {
	t.SkipNow()

	conn := newMemoryConnection()

	userIdents, err := conn.ResolveIdents([]any{
		"appUser/firstName",
		"appUser/lastName",
		"appUser/type",
		"appUser/type.customer",
		"appUser/type.guest",
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, userIdents, 5)

	ids := make(map[store.ID]struct{})
	for _, ident := range userIdents {
		ids[ident.ID] = struct{}{}
	}
	assert.Len(t, ids, len(userIdents), "each ident should be allocated a unique ID")

	// Fetch the same idents. This time, supply the names in a different order
	// to ensure that the IDs allocated are actually associated with the name
	// supplied.
	reFetchedUserIdents, err := conn.ResolveIdents([]any{
		"appUser/type.customer",
		"appUser/firstName",
		"appUser/type.guest",
		"appUser/type",
		"appUser/lastName",
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.ElementsMatch(t, userIdents, reFetchedUserIdents, "should fetch correct already-allocated idents")

	ident := reFetchedUserIdents[0]
	fetchedByID, err := conn.ResolveIdents([]any{ident.ID})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, ident, fetchedByID[0], "should fetch the expected ident by ID")

	mixedFetchedAndAllocated, err := conn.ResolveIdents([]any{
		// By name
		reFetchedUserIdents[0].Name,
		// By ID
		reFetchedUserIdents[1].ID,
		// Already resolved
		reFetchedUserIdents[2],
		// Should allocate
		"foo/test",
	})

	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, mixedFetchedAndAllocated, 4)
	assert.Equal(t, reFetchedUserIdents[:3], mixedFetchedAndAllocated[:3])
	newlyAllocatedIdent := mixedFetchedAndAllocated[3]
	assert.Equal(t, "foo/test", newlyAllocatedIdent.Name)
	_, existing := ids[newlyAllocatedIdent.ID]
	assert.False(t, existing, "new ID should have been allocated")
}

func TestAssert(t *testing.T) {
	// Use the entity API to assert facts about the schema.
	conn := newMemoryConnection()
	// db := conn.DB()
	res, err := conn.Assert(
		store.EntityData{
			"db/ident":       "person/email",
			"db/type":        "db.type/string",
			"db/unique":      true,
			"db/cardinality": "db.cardinality/one",
		},
	)
	if !assert.NoError(t, err) {
		return
	}
	assert.NotNil(t, res)

	eid, err := store.Ident{
		Name: "person/email",
	}.Resolve(conn)
	if !assert.NoError(t, err, "should resolve ident for newly created schema entity") {
		return
	}

	// Fetch the entity and assert that it has the expected properties.
	entity, err := conn.GetEntity(eid)
	if !assert.NoError(t, err) {
		return
	}
	data, err := entity.GetData(conn)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, store.EntityData{
		"db/ident":       store.Ident{Name: "person/email"}.MustResolve(conn),
		"db/type":        store.Ident{Name: "db.type/string"}.MustResolve(conn),
		"db/unique":      true,
		"db/cardinality": store.Ident{Name: "db.cardinality/one"}.MustResolve(conn),
	}, data)
}

func TestAssertExistingEntity(t *testing.T) {
	conn := newTestConn()
	{
		res, err := conn.Assert(
			store.EntityData{
				"person/email":     "ameredith@example.com",
				"person/firstName": "Andrew",
			},
		)
		if !assert.NoError(t, err) {
			return
		}
		assert.NotNil(t, res)
	}

	{
		res, err := conn.Assert(
			store.EntityData{
				"person/email":    "ameredith@example.com",
				"person/lastName": "Meredith",
			},
		)
		assert.NoError(t, err)
		assert.NotNil(t, res)
	}

	{
		entity, err := conn.GetEntity(store.NewLookup("person/email", "ameredith@example.com"))
		if !assert.NoError(t, err) {
			return
		}
		data, err := entity.GetData(conn)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, store.EntityData{
			"person/email":     "ameredith@example.com",
			"person/firstName": "Andrew",
			"person/lastName":  "Meredith",
		}, data)
	}
}

func TestMultipleUniqueIdentifiers(t *testing.T) {
	conn := newTestConn()

	// Create two entities with both email address and ssn.
	{
		res, err := conn.Assert(
			store.EntityData{
				"person/email":     "bob@example.com",
				"person/ssn":       "123-45-6789",
				"person/firstName": "Bob",
			},
		)
		if !assert.NoError(t, err) {
			return
		}
		assert.NotNil(t, res)
	}

	// Make an assertion using only the email address.
	{
		res, err := conn.Assert(
			store.EntityData{
				"person/email":    "bob@example.com",
				"person/lastName": "Smith",
			},
		)
		if !assert.NoError(t, err) {
			return
		}
		assert.NotNil(t, res)
	}

	// Make an assertion using only the ssn.
	var resolvedPetID store.ID
	{
		petID := store.TempID()
		res, err := conn.Assert(
			store.EntityData{
				"person/ssn":  "123-45-6789",
				"person/pets": []any{petID},
			},
			store.EntityData{
				"db/id":     petID,
				"pet/name":  "Sir Wimbledon",
				"pet/breed": "Whippet",
			},
		)
		if !assert.NoError(t, err) {
			return
		}
		assert.NotNil(t, res)

		var ok bool
		resolvedPetID, ok = res.TempIDs.LookupTempID(petID)
		if !assert.True(t, ok, "should resolve temp ID to permanent ID") {
			return
		}
	}

	// Fetch the entity by email address and ensure that it has the expected properties.
	{
		entity, err := conn.GetEntity(store.NewLookup("person/email", "bob@example.com"))
		if !assert.NoError(t, err) {
			return
		}
		data, err := entity.GetData(conn)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, store.EntityData{
			"person/email":     "bob@example.com",
			"person/ssn":       "123-45-6789",
			"person/firstName": "Bob",
			"person/lastName":  "Smith",
			"person/pets":      []store.Value{resolvedPetID},
		}, data)
	}

	// Ensure the SSN and Email resolve to the same entity.
	{
		eidForEmail, err := store.NewLookup("person/email", "bob@example.com").Resolve(conn)
		if !assert.NoError(t, err) {
			return
		}
		eidForSSN, err := store.NewLookup("person/ssn", "123-45-6789").Resolve(conn)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, eidForEmail, eidForSSN, "email and ssn should resolve to the same entity")
	}

	// Failure case: make an assertion using the same ssn but a different email address.
	// TODO: Make this test pass.
	// {
	// 	_, err := conn.Assert(
	// 		store.EntityData{
	// 			"person/email": "robert.smith@example.com",
	// 			"person/ssn":   "123-45-6789",
	// 		},
	// 	)
	// 	assert.Error(t, err, "should not be able to assert a different email address for the same ssn")
	// }
}

// newTestConn returns a new connection to an in-memory test store
// that has been initialized with the schema required for testing.
func newTestConn() *store.Connection {
	conn := newMemoryConnection()
	// db := conn.DB()
	// Create the schema.
	_, err := conn.Assert(
		// Person
		store.EntityData{
			"db/ident":       "person/email",
			"db/type":        "db.type/string",
			"db/unique":      true,
			"db/cardinality": "db.cardinality/one",
			"db/doc":         "An individual email address. Used to uniquely identify a person.",
		},
		store.EntityData{
			"db/ident":       "person/ssn",
			"db/type":        "db.type/string",
			"db/unique":      true,
			"db/cardinality": "db.cardinality/one",
			"db/doc":         "A Social Security Number. Used to uniquely identify a person.",
		},
		store.EntityData{
			"db/ident":       "person/firstName",
			"db/type":        "db.type/string",
			"db/cardinality": "db.cardinality/one",
		},
		store.EntityData{
			"db/ident":       "person/lastName",
			"db/type":        "db.type/string",
			"db/cardinality": "db.cardinality/one",
		},
		store.EntityData{
			"db/ident":       "person/pets",
			"db/type":        "db.type/ref",
			"db/cardinality": "db.cardinality/many",
		},
		// Pet
		store.EntityData{
			"db/ident":       "pet/id",
			"db/type":        "db.type/string",
			"db/unique":      true,
			"db/cardinality": "db.cardinality/one",
		},
		store.EntityData{
			"db/ident":       "pet/name",
			"db/type":        "db.type/string",
			"db/cardinality": "db.cardinality/one",
		},
		store.EntityData{
			"db/ident":       "pet/breed",
			"db/type":        "db.type/string",
			"db/cardinality": "db.cardinality/one",
		},
	)
	if err != nil {
		panic(err)
	}
	return conn
}

func newMemoryConnection() *store.Connection {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		panic(err)
	}
	sto, err := badgerImpl.New(db)
	if err != nil {
		panic(err)
	}
	p := store.NewConnection(store.Config{
		IdentManager: sto,
		IDManager:    sto,
		Indexer:      sto,
	})
	p.InitializeDB()

	return p
}
