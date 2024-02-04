package store

import (
	"context"
	"fmt"

	"github.com/kendru/canter/pkg/dataflow"
)

type Lookup struct {
	AttributeName string
	Value         Value
}

func NewLookup(attributeName string, value Value) Lookup {
	return Lookup{
		AttributeName: attributeName,
		Value:         value,
	}
}

func (l Lookup) Resolve(conn *Connection) (ID, error) {
	attr, err := ResolveIdent(conn, l.AttributeName)
	if err != nil {
		return 0, fmt.Errorf("resolving attribute: %w", err)
	}
	schemaEntity, err := conn.GetEntity(attr.ID)
	if err != nil {
		return 0, fmt.Errorf("fetching attribute schema: %w", err)
	}
	isUnique, err := schemaEntity.Get(conn, IDUnique)
	switch err {
	case nil:
		if !isUnique.(bool) {
			return 0, fmt.Errorf("attribute %q is not unique", l.AttributeName)
		}
	case ErrPropertyNotFound:
		return 0, fmt.Errorf("attribute %q is not unique", l.AttributeName)
	default:
		return 0, fmt.Errorf("fetching attribute %q uniqueness: %w", l.AttributeName, err)
	}

	scan, err := conn.indexer.ScanAVET(attr.ID, l.Value)
	if err != nil {
		return 0, fmt.Errorf("scanning AVET index to resolve Lookup: %w", err)
	}

	facts, err := dataflow.CollectIntoSlice(dataflow.NewContext(context.Background()), scan)
	if err != nil {
		return 0, fmt.Errorf("scanning AVET index to resolve Lookup: %w", err)
	}

	if len(facts) == 0 {
		return 0, ErrNoSuchEntity
	}

	return facts[0].EntityID, nil
}
