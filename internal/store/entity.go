package store

import (
	"errors"
	"fmt"
	"reflect"
)

var ErrPropertyNotFound = errors.New("property not found")

// Entity is an immutable record that contains that state of all attributes
// associated with a particular ID at some point in time.
type Entity struct {
	eid     ID
	basisID ID
	state   map[ID]Value
}

func (e Entity) ID() ID {
	return e.eid
}

// Get ... attribute may either be a string attribute ident or a resolved ID
// representing the attribute to retrieve.
func (e Entity) Get(conn *Connection, attribute any) (Value, error) {
	attrIdent, err := ResolveIdent(conn, attribute)
	if err != nil {
		return nil, fmt.Errorf("resolving attribute ident: %w", err)
	}
	val, ok := e.state[attrIdent.ID]
	if !ok {
		return nil, ErrPropertyNotFound
	}

	return val, nil
}

func (e Entity) GetData(conn *Connection) (EntityData, error) {
	attrIDs := make([]any, 0, len(e.state))
	data := make(EntityData, len(e.state))
	for attrID := range e.state {
		attrIDs = append(attrIDs, attrID)
	}
	idents, err := conn.ResolveIdents(attrIDs)
	if err != nil {
		return nil, fmt.Errorf("resolving attribute idents: %w", err)
	}
	for i, ident := range idents {
		id := attrIDs[i]
		data[ident.Name] = e.state[id.(ID)]
	}

	return data, nil

}

type EntityData map[string]Value

// Assertions implements Assertable for EntityData.
// This method resolves attribute idents because it needs  and
func (ed EntityData) Assertions(conn *Connection) ([]Assertion, error) {
	// Resolve EntityID or generate TempID.
	id, err := ed.getIdentifier(conn)
	if err != nil {
		return nil, fmt.Errorf("resolving EntityID for EntityData: %w", err)
	}

	assertions := make([]Assertion, 0, len(ed))

	for attrIdentName, val := range ed {
		if attrIdentName == "db/id" {
			// ID only used to match existing entity.
			continue
		}

		rv := reflect.ValueOf(val)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			// Split multi-valued attributes into multiple assertions.
			for i := 0; i < rv.Len(); i++ {
				assertions = append(assertions, Assert(
					id,
					attrIdentName,
					rv.Index(i).Interface(),
				))
			}

		default:
			assertions = append(assertions, Assert(
				id,
				attrIdentName,
				val,
			))
		}
	}

	return assertions, nil
}

func (ed EntityData) getIdentifier(conn *Connection) (identifier, error) {
	// Resolve EntityID or generate TempID.
	for attrIdentName, val := range ed {
		switch attrIdentName {
		case "db/id":
			// TODO: ensure that an entity with this id already exists.
			return val.(identifier), nil
		}
		if attrIdentName == "db/ident" {
			valIdent, err := ResolveIdent(conn, val)
			if err != nil {
				if errors.Is(err, ErrNoSuchIdent) {
					return TempID(), nil
				}
				return nil, fmt.Errorf("resolving db/ident value as ident: %w", err)
			}
			return valIdent.ID, nil
		}
		// TODO Resolve a lookup.
		// TODO: Attempt to resolve a unique attribute to an entityID as well.
		// If attributes contain any combination of db/id and unique attributes
		// that resolve to different IDs, throw an error.
	}

	return TempID(), nil
}
