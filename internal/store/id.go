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
