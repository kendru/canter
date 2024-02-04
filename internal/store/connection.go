package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/kendru/canter/internal/util"
	"github.com/kendru/canter/pkg/dataflow"
	"github.com/oklog/ulid/v2"
)

const unresolvedEntityID = ID(0)

type Config struct {
	IdentManager
	IDManager
	Indexer
}

func NewConnection(cfg Config) *Connection {
	// Initialize an ident cache that is hydrated with system idents.
	identCache := newIdentCache(cfg.IdentManager)

	// TODO: Figure out when to call this and how to handle errors.
	go func() {
		idents, err := cfg.IdentManager.LoadIdents()
		if err != nil {
			println("Error loading idents from ident manager:", err)
			return
		}
		identCache.store(idents)
	}()

	return &Connection{
		identCache:        identCache,
		identManager:      cfg.IdentManager,
		schemaEntityCache: make(map[ID]Entity),
		idManager:         cfg.IDManager,
		indexer:           cfg.Indexer,
	}
}

// Connection is the structure used to maintain
type Connection struct {
	// ident
	identManager IdentManager
	identCache   *identCache

	// schema
	schemaEntityCache map[ID]Entity

	idManager IDManager

	indexer Indexer
}

// InitializeDB sets up all of the required resources in the underlying storage
// engine for the database to which the peer is connected. It should only be
// called once, before transacting any data.
func (conn *Connection) InitializeDB() error {
	// TODO: Ensure system is not already initialized.

	// Note that since we do not have any schema in the database already, we
	// must call the internal `conn.assert()`, passing ResolvedAssertions, which
	// use IDs directly rather than attempting to resolve idents, lookups, etc.
	assertions := make([]ResolvedAssertion, 0, 256)

	// Allocate a transaction ID for the initial transaction, and assert the transaction timestamp fact.
	var txID ID
	var err error
	for txID == 0 {
		txID, err = conn.idManager.NextID()
		if err != nil {
			return fmt.Errorf("getting ID for initial transaction: %w", err)
		}
	}

	assertions = append(assertions, ResolvedAssertion{
		Fact: Fact{
			EntityID:  txID,
			Attribute: IDTxCommitTime,
			Value:     uint64(time.Now().Unix()),
			Tx:        txID,
		},
		mode: AssertModeAddition,
	})

	// Add system schema.
	schemaEntities := []map[ID]any{
		{
			IDIdent:       IDID,
			IDType:        IDTypeInt64,
			IDCardinality: IDCardinalityOne,
			IDUnique:      true,
			IDDoc:         "Entity ID",
		},
		{
			IDIdent:       IDIdent,
			IDType:        IDTypeRef,
			IDCardinality: IDCardinalityOne,
			IDUnique:      true,
			IDDoc:         "Global ident. Should be applied to schema entities and global values like enum variants.",
		},
		{
			IDIdent:       IDType,
			IDType:        IDTypeRef,
			IDCardinality: IDCardinalityOne,
			IDDoc:         "Schema entity type",
		},
		// TODO: Add IDCompositeComponents schema entity.
		{
			IDIdent:       IDCardinality,
			IDType:        IDTypeRef,
			IDCardinality: IDCardinalityOne,
			IDDoc:         "Cardinality of an attribute. Enumerated value: db.cardinality/one or db.cardinality/many",
		},
		{
			IDIdent:       IDUnique,
			IDType:        IDTypeBoolean,
			IDCardinality: IDCardinalityOne,
			IDDoc:         "Whether an attribute is unique. If true, only one entity may have a given value for the attribute.",
		},
		{
			IDIdent:       IDIndexed,
			IDType:        IDTypeBoolean,
			IDCardinality: IDCardinalityOne,
			IDDoc:         "Whether an attribute is indexed. If true, the attribute will be indexed in the AVET index.",
		},
		{
			IDIdent:       IDDoc,
			IDType:        IDTypeString,
			IDCardinality: IDCardinalityOne,
			IDDoc:         "Documentation for an attribute or entity.",
		},
		{
			IDIdent:       IDTxCommitTime,
			IDType:        IDTypeTimestamp,
			IDCardinality: IDCardinalityOne,
			IDDoc:         "Timestamp of the transaction commit.",
		},
		// Enum values.
		{
			IDIdent: IDCardinalityOne,
		},
		{
			IDIdent: IDCardinalityMany,
		},
		{
			IDIdent: IDTypeString,
		},
		{
			IDIdent: IDTypeBoolean,
		},
		{
			IDIdent: IDTypeInt64,
		},
		{
			IDIdent: IDTypeInt32,
		},
		{
			IDIdent: IDTypeInt16,
		},
		{
			IDIdent: IDTypeInt8,
		},
		{
			IDIdent: IDTypeFloat64,
		},
		{
			IDIdent: IDTypeFloat32,
		},
		{
			IDIdent: IDTypeDecimal,
		},
		{
			IDIdent: IDTypeTimestamp,
		},
		{
			IDIdent: IDTypeDate,
		},
		{
			IDIdent: IDTypeRef,
		},
		{
			IDIdent: IDTypeBinary,
		},
		{
			IDIdent: IDTypeUUID,
		},
		{
			IDIdent: IDTypeULID,
		},
		{
			IDIdent: IDTypeComposite,
		},
	}

	for _, entityData := range schemaEntities {
		var eid ID
		for key, val := range entityData {
			if key == IDIdent {
				eid = val.(ID)
				break
			}
		}
		for attr, value := range entityData {
			assertions = append(assertions, ResolvedAssertion{
				Fact: Fact{
					EntityID:  eid,
					Attribute: attr,
					Value:     value,
					Tx:        txID,
				},
				mode: AssertModeAddition,
			})
		}
	}

	if _, err := conn.assert(Database{}, assertions, nil); err != nil {
		return fmt.Errorf("asserting initial data: %w", err)
	}

	return nil
}

// ResolveIdents takes a slice of arguments, each of which may be an
// already-resolved ID or a string ident. It resolves the arguments to their
// canonical form. The resulting `ids` will be parallel to the given `idents`.
// This method will not allocate, and an error will be returned if any of the
// arguments cannot be resolved to an ident.
func (conn *Connection) ResolveIdents(idents []any) (ids []Ident, err error) {
	out := make([]Ident, len(idents))
	// Maps index of resolved ident to index in `out`
	var unresolvedNames []string
	var unresolvedIDs []ID

	var indexesNames []int
	var indexesIDs []int

	for idx, ident := range idents {
		switch v := ident.(type) {
		case Ident:
			// Already resolved.
			out[idx] = v

		case ID:
			// We have an ID. Assume this was loaded from the db and is valid.
			if ident, ok := conn.identCache.lookupByID(v); ok {
				out[idx] = ident
			} else {
				unresolvedIDs = append(unresolvedIDs, v)
				indexesIDs = append(indexesIDs, idx)
			}

		case string:
			// We have a name. This may map to an existing ident or may need to
			// be loaded from the ident manager and cached.
			if ident, ok := conn.identCache.lookupByName(v); ok {
				out[idx] = ident
			} else {
				if strings.HasPrefix(v, "db/") {
					return nil, errors.New(`the "db" namespace is reserved for system identifiers`)
				}
				unresolvedNames = append(unresolvedNames, v)
				indexesNames = append(indexesNames, idx)
			}

		default:
			return nil, fmt.Errorf("type at [%d] cannot resolve to an ident: %T", idx, ident)
		}
	}

	// Resolve unknown names.
	if len(unresolvedNames) > 0 {
		ids, err := conn.identManager.LookupIdentIDs(unresolvedNames)
		if err != nil {
			return nil, err
		}
		newIdents := make([]Ident, len(ids))
		for idx, id := range ids {
			name := unresolvedNames[idx]
			outIdx := indexesNames[idx]
			newIdent := Ident{
				ID:   id,
				Name: name,
			}
			newIdents[idx] = newIdent
			out[outIdx] = newIdent
		}
		// Cache the new idents
		conn.identCache.store(newIdents)
	}

	// Resolve unknown IDs.
	if len(unresolvedIDs) > 0 {
		names, err := conn.identManager.LookupIdentNames(unresolvedIDs)
		if err != nil {
			return nil, err
		}
		newIdents := make([]Ident, len(names))
		for idx, name := range names {
			id := unresolvedIDs[idx]
			outIdx := indexesIDs[idx]
			newIdent := Ident{
				ID:   id,
				Name: name,
			}
			newIdents[idx] = newIdent
			out[outIdx] = newIdent
		}
		conn.identCache.store(newIdents)
	}

	return out, nil
}

func ResolveIdent(conn *Connection, ident any) (Ident, error) {
	idents, err := conn.ResolveIdents([]any{ident})
	if err != nil {
		return NullIdent, err
	}
	return idents[0], nil
}

type TempIDs map[string]ID

func (ids TempIDs) LookupTempID(tid tempID) (ID, bool) {
	id, ok := ids[tid.symbol]
	return id, ok
}

type AssertResult struct {
	DB      Database
	Data    []ResolvedAssertion
	TempIDs TempIDs
}

func (conn *Connection) Assert(assertables ...Assertable) (*AssertResult, error) {
	var assertions []Assertion

	for _, a := range assertables {
		newAssertions, err := a.Assertions(conn)
		if err != nil {
			return nil, fmt.Errorf("resolving facts for assertion: %w", err)
		}
		assertions = append(assertions, newAssertions...)
	}

	// Return if any assertions have validation errors.
	assertionErrors := util.Map(assertions, func(assertion Assertion) error {
		return assertion.err
	})
	if err := errors.Join(assertionErrors...); err != nil {
		return nil, fmt.Errorf("invalid assertions: %w", err)
	}

	// Create a map of tempID symbols to their resolved IDs.
	tempIDs := make(TempIDs)

	// Append assertions for transaction.
	// Create a transaction entity, using a tempID with a well-known symbol.
	tempIDs["txid"] = unresolvedEntityID // TODO: ensure that tx ids are monotonically increasing, regardless of which instance assigned them.
	assertions = append(assertions, Assertion{
		entityID:  tempID{symbol: "txid"},
		attribute: "db.tx/commitTime",
		value:     uint64(time.Now().Unix()), // TODO: Get time from database.
	})
	isIDConflict := func(sym string, newID ID) bool {
		resolvedID, ok := tempIDs[sym]
		return ok &&
			resolvedID != unresolvedEntityID &&
			resolvedID != newID
	}

	// First pass:
	// 1. Collect tempIDs in the ID and Value positions.
	// 2. Resolve lookups in the Value position.
	// 3. Resolve idents in all positions.
	for idx := range assertions {
		assertion := &assertions[idx]

		/////////////////
		// Attribute Resolution

		// Resolve Attribute to an ID.
		attribute, err := ResolveIdent(conn, assertion.attribute)
		if err != nil {
			return nil, err
		}
		assertion.attribute = attribute.ID

		/////////////////
		// Value Resolution

		// Get schema ident. Value resolution is dependent on the type that the
		// attributes refers to.
		schemaEntity, err := conn.getSchemaEntity(attribute.ID)
		if err != nil {
			return nil, fmt.Errorf("fetching attribute schema: %w", err)
		}
		var valueTypeID ID
		valueType, err := schemaEntity.Get(conn, IDType)
		switch err {
		case nil:
			valueTypeID = valueType.(ID)
		case ErrPropertyNotFound:
			return nil, fmt.Errorf("attribute entity %d is not a schema entity", attribute.ID)
		default:
			return nil, err
		}

		// Resolve value based on attribute type.
		// TODO: Extract this to a function.
		switch valueTypeID {
		case IDTypeRef:
			if asStr, ok := assertion.value.(string); ok {
				assertion.value = Ident{Name: asStr}
			}

			switch v := assertion.value.(type) {
			case ID:
				// Nothing to do - value is already an ID.
			case tempID:
				// Add to tempIDs map if not already present.
				if _, ok := tempIDs[v.symbol]; !ok {
					tempIDs[v.symbol] = unresolvedEntityID
				}
			default:
				// Resolve lookups and idents in the Value position.
				asResolver, ok := assertion.value.(Resolver)
				if !ok {
					return nil, fmt.Errorf("value for ref attribute %q must resolve to an ID", attribute.Name)
				}
				resolvedID, err := asResolver.Resolve(conn)
				if err != nil {
					// Special case: if this is an id/ident we have not yet
					// seen, we can allocate a new ID for the ident. The
					// subsequent EntityID resolution pass will resolve the
					// tempID for all attributes in this entity to the new ID.
					if attribute.ID == IDIdent && errors.Is(err, ErrNoSuchIdent) {
						// Safety: only a resolver for an Ident can return ErrNoSuchIdent.
						id, err := conn.idManager.NextID()
						if err != nil {
							return nil, fmt.Errorf("allocating new ID for db/ident: %w", err)
						}
						resolvedID = id
						asIdent := assertion.value.(Ident)
						asIdent.ID = id
						conn.identManager.StoreIdent(asIdent)
					} else {
						return nil, fmt.Errorf("resolving value of ref attribute %q: %w", attribute.Name, err)
					}
				}
				assertion.value = resolvedID
			}

		case IDTypeString:
			switch v := assertion.value.(type) {
			case string:
				// Nothing to do - value is already a string.
			case []byte:
				assertion.value = string(v)
			default:
				return nil, fmt.Errorf("value for string attribute %q is not assignable to a string", attribute.Name)
			}

		case IDTypeInt64:
			switch v := assertion.value.(type) {
			case int64:
				// Nothing to do - value is already an int64.
			case uint64:
				assertion.value = int64(v)
			case int:
				assertion.value = int64(v)
			case uint:
				assertion.value = int64(v)
			case int32:
				assertion.value = int64(v)
			case uint32:
				assertion.value = int64(v)
			case int16:
				assertion.value = int64(v)
			case uint16:
				assertion.value = int64(v)
			case int8:
				assertion.value = int64(v)
			case uint8:
				assertion.value = int64(v)
			default:
				return nil, fmt.Errorf("value for int64 attribute %q is not assignable to an int64", attribute.Name)
			}

		case IDTypeInt32:
			switch v := assertion.value.(type) {
			case int64:
				if v > math.MaxInt32 || v < math.MinInt32 {
					return nil, fmt.Errorf("value for int32 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int32(v)
			case uint64:
				if v > math.MaxInt32 {
					return nil, fmt.Errorf("value for int32 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int32(v)
			case int:
				if v > math.MaxInt32 || v < math.MinInt32 {
					return nil, fmt.Errorf("value for int32 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int32(v)
			case uint:
				if v > math.MaxInt32 {
					return nil, fmt.Errorf("value for int32 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int32(v)
			case int32:
				// Nothing to do - value is already an int32.
			case uint32:
				if v > math.MaxInt32 {
					return nil, fmt.Errorf("value for int32 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int32(v)
			case int16:
				assertion.value = int32(v)
			case uint16:
				assertion.value = int32(v)
			case int8:
				assertion.value = int32(v)
			case uint8:
				assertion.value = int32(v)
			default:
				return nil, fmt.Errorf("value for int32 attribute %q is not assignable to an int32", attribute.Name)
			}

		case IDTypeInt16:
			switch v := assertion.value.(type) {
			case int64:
				if v > math.MaxInt16 || v < math.MinInt16 {
					return nil, fmt.Errorf("value for int16 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int16(v)
			case uint64:
				if v > math.MaxInt16 {
					return nil, fmt.Errorf("value for int16 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int16(v)
			case int:
				if v > math.MaxInt16 || v < math.MinInt16 {
					return nil, fmt.Errorf("value for int16 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int16(v)
			case uint:
				if v > math.MaxInt16 {
					return nil, fmt.Errorf("value for int16 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int16(v)
			case int32:
				if v > math.MaxInt16 || v < math.MinInt16 {
					return nil, fmt.Errorf("value for int16 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int16(v)
			case uint32:
				if v > math.MaxInt16 {
					return nil, fmt.Errorf("value for int16 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int16(v)
			case int16:
				// Nothing to do - value is already an int16.
			case uint16:
				if v > math.MaxInt16 {
					return nil, fmt.Errorf("value for int16 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int16(v)
			case int8:
				assertion.value = int16(v)
			case uint8:
				assertion.value = int16(v)
			default:
				return nil, fmt.Errorf("value for int16 attribute %q is not assignable to an int16", attribute.Name)
			}

		case IDTypeInt8:
			switch v := assertion.value.(type) {
			case int64:
				if v > math.MaxInt8 || v < math.MinInt8 {
					return nil, fmt.Errorf("value for int8 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int8(v)
			case uint64:
				if v > math.MaxInt8 {
					return nil, fmt.Errorf("value for int8 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int8(v)
			case int:
				if v > math.MaxInt8 || v < math.MinInt8 {
					return nil, fmt.Errorf("value for int8 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int8(v)
			case uint:
				if v > math.MaxInt8 {
					return nil, fmt.Errorf("value for int8 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int8(v)
			case int32:
				if v > math.MaxInt8 || v < math.MinInt8 {
					return nil, fmt.Errorf("value for int8 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int8(v)
			case uint32:
				if v > math.MaxInt8 {
					return nil, fmt.Errorf("value for int8 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int8(v)
			case int16:
				if v > math.MaxInt8 || v < math.MinInt8 {
					return nil, fmt.Errorf("value for int8 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int8(v)
			case uint16:
				if v > math.MaxInt8 {
					return nil, fmt.Errorf("value for int8 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int8(v)
			case int8:
				// Nothing to do - value is already an int8.
			case uint8:
				if v > math.MaxInt8 {
					return nil, fmt.Errorf("value for int8 attribute %q is out of range", attribute.Name)
				}
				assertion.value = int8(v)

			default:
				return nil, fmt.Errorf("value for int8 attribute %q is not assignable to an int8", attribute.Name)
			}

		case IDTypeBoolean:
			switch assertion.value.(type) {
			case bool:
				// Nothing to do - value is already a bool.
			default:
				return nil, fmt.Errorf("value for boolean attribute %q is not assignable to a bool", attribute.Name)
			}

		case IDTypeFloat64:
			switch v := assertion.value.(type) {
			case float64:
				// Nothing to do - value is already a float64.
			case float32:
				assertion.value = float64(v)
			case int64:
				assertion.value = float64(v)
			case uint64:
				assertion.value = float64(v)
			case int:
				assertion.value = float64(v)
			case uint:
				assertion.value = float64(v)
			case int32:
				assertion.value = float64(v)
			case uint32:
				assertion.value = float64(v)
			case int16:
				assertion.value = float64(v)
			case uint16:
				assertion.value = float64(v)
			case int8:
				assertion.value = float64(v)
			case uint8:
				assertion.value = float64(v)
			default:
				return nil, fmt.Errorf("value for float64 attribute %q is not assignable to a float64", attribute.Name)
			}

		case IDTypeFloat32:
			switch v := assertion.value.(type) {
			case float64:
				if v > math.MaxFloat32 || v < -math.MaxFloat32 {
					return nil, fmt.Errorf("value for float32 attribute %q is out of range", attribute.Name)
				}
				assertion.value = float32(v)
			case float32:
				// Nothing to do - value is already a float32.
			case int64:
				assertion.value = float32(v)
			case uint64:
				assertion.value = float32(v)
			case int:
				assertion.value = float32(v)
			case uint:
				assertion.value = float32(v)
			case int32:
				assertion.value = float32(v)
			case uint32:
				assertion.value = float32(v)
			case int16:
				assertion.value = float32(v)
			case uint16:
				assertion.value = float32(v)
			case int8:
				assertion.value = float32(v)
			case uint8:
				assertion.value = float32(v)
			default:
				return nil, fmt.Errorf("value for float32 attribute %q is not assignable to a float32", attribute.Name)
			}

		case IDTypeTimestamp:
			switch v := assertion.value.(type) {
			case time.Time:
				// Nothing to do - value is already a time.Time.
			case int64:
				assertion.value = time.Unix(v, 0)
			case uint64:
				assertion.value = time.Unix(int64(v), 0)
			case int:
				assertion.value = time.Unix(int64(v), 0)
			case uint:
				assertion.value = time.Unix(int64(v), 0)
			case int32:
				assertion.value = time.Unix(int64(v), 0)
			case uint32:
				assertion.value = time.Unix(int64(v), 0)
			case int16:
				assertion.value = time.Unix(int64(v), 0)
			case uint16:
				assertion.value = time.Unix(int64(v), 0)
			case int8:
				assertion.value = time.Unix(int64(v), 0)
			case uint8:
				assertion.value = time.Unix(int64(v), 0)
			case string:
				t, err := time.Parse(time.RFC3339, v)
				if err != nil {
					return nil, fmt.Errorf("value for timestamp attribute %q is not a valid RFC3339 string", attribute.Name)
				}
				assertion.value = t
			default:
				return nil, fmt.Errorf("value for timestamp attribute %q is not assignable to a time.Time", attribute.Name)
			}

		case IDTypeDate:
			var t time.Time
			switch v := assertion.value.(type) {
			case time.Time:
				// Nothing to do - value is already a time.Time.
			case int64:
				t = time.Unix(v, 0)
			case uint64:
				t = time.Unix(int64(v), 0)
			case int:
				t = time.Unix(int64(v), 0)
			case uint:
				t = time.Unix(int64(v), 0)
			case int32:
				t = time.Unix(int64(v), 0)
			case uint32:
				t = time.Unix(int64(v), 0)
			case int16:
				t = time.Unix(int64(v), 0)
			case uint16:
				t = time.Unix(int64(v), 0)
			case int8:
				t = time.Unix(int64(v), 0)
			case uint8:
				t = time.Unix(int64(v), 0)
			case string:
				parsedTime, err := time.Parse("2006-01-02", v)
				if err != nil {
					return nil, fmt.Errorf("value for date attribute %q is not a valid date string (YYYY-MM-DD)", attribute.Name)
				}
				t = parsedTime
			default:
				return nil, fmt.Errorf("value for date attribute %q is not assignable to a time.Time", attribute.Name)
			}
			assertion.value = t.UTC().Truncate(24 * time.Hour)

		case IDTypeBinary:
			switch v := assertion.value.(type) {
			case []byte:
				// Nothing to do - value is already a []byte.
			case string:
				assertion.value = []byte(v)
			default:
				return nil, fmt.Errorf("value for binary attribute %q is not assignable to a []byte", attribute.Name)
			}

		case IDTypeDecimal:
			panic("TODO: decimal type not implemented")

		case IDTypeComposite:
			panic("TODO: composite type not implemented")

		case IDTypeUUID:
			switch v := assertion.value.(type) {
			case uuid.UUID:
				// Nothing to do - value is already a uuid.UUID.
			case string:
				parsedUUID, err := uuid.FromString(v)
				if err != nil {
					return nil, fmt.Errorf("value for uuid attribute %q is not a valid uuid string", attribute.Name)
				}
				assertion.value = parsedUUID
			case []byte:
				parsedUUID, err := uuid.FromBytes(v)
				if err != nil {
					return nil, fmt.Errorf("value for uuid attribute %q is not a valid uuid byte slice", attribute.Name)
				}
				assertion.value = parsedUUID
			default:
				return nil, fmt.Errorf("value for uuid attribute %q is not assignable to a uuid.UUID", attribute.Name)
			}

		case IDTypeULID:
			switch v := assertion.value.(type) {
			case ulid.ULID:
				// Nothing to do - value is already a ulid.ULID.
			case string:
				parsedULID, err := ulid.Parse(v)
				if err != nil {
					return nil, fmt.Errorf("value for ulid attribute %q is not a valid ulid string", attribute.Name)
				}
				assertion.value = parsedULID
			default:
				return nil, fmt.Errorf("value for ulid attribute %q is not assignable to a ulid.ULID", attribute.Name)
			}

		default:
			panic(fmt.Sprintf("unhandled attribute type: %s", valueTypeID))
		}

		/////////////////
		// ID Resolution

		// Mark tempIDs for resolution, and resolve idents and lookups.
		switch v := assertion.entityID.(type) {
		case ID:
			// Already resolved.

		case tempID:
			// Special cases for ID resolution of tempIDs.
			switch assertion.attribute {
			case IDID:
				// ID was specified as db/id.
				id, ok := assertion.value.(ID)
				if !ok {
					return nil, fmt.Errorf("value for db/id must resolve to an ID")
				}
				scan, err := conn.indexer.ScanEAVT(attribute.ID, &id)
				if err != nil {
					return nil, fmt.Errorf("scanning for existing entity with db/id %d: %w", id, err)
				}
				facts, err := dataflow.CollectIntoSlice(dataflow.NewContext(context.Background()), scan)
				if err != nil {
					return nil, fmt.Errorf("scanning for existing entity with db/id %d: %w", id, err)
				}
				if len(facts) == 0 {
					return nil, fmt.Errorf("no entity found with db/id %d", id)
				}
				if isIDConflict(v.symbol, id) {
					return nil, errors.Join(
						fmt.Errorf("db/id %d conflicts with an already-resolved ID for tempid %q", id, v.symbol),
						ErrConflict,
					)
				}
				tempIDs[v.symbol] = id

			case IDIdent:
				// ID was allocated on the first pass through the assertions.
				id := assertion.value.(ID)
				if isIDConflict(v.symbol, id) {
					return nil, errors.Join(
						fmt.Errorf("db/ident %q conflicts with an already-resolved ID for tempid %s", assertion.value, v.symbol),
						ErrConflict,
					)
				}
				tempIDs[v.symbol] = id

			default:
				// If unique attribute, resolve to an ID.
				isUnique, err := schemaEntity.Get(conn, IDUnique)
				if err != nil && !errors.Is(err, ErrPropertyNotFound) {
					return nil, fmt.Errorf("fetching attribute schema: %w", err)
				}
				if isUnique != nil && isUnique.(bool) {
					id, err := NewLookup(attribute.Name, assertion.value).Resolve(conn)
					switch err {
					case nil:
						if isIDConflict(v.symbol, id) {
							return nil, errors.Join(
								fmt.Errorf("unique attribute %q conflicts with an already-resolved ID for tempid %q", attribute.Name, v.symbol),
								ErrConflict,
							)
						}
						tempIDs[v.symbol] = id
					case ErrNoSuchEntity:
						// This is fine - we will create a new entity.
					default:
						return nil, fmt.Errorf("resolving lookup: %w", err)
					}
				}

				// HAPPY PATH:
				// Add to tempIDs map if not already present.
				if _, ok := tempIDs[v.symbol]; !ok {
					tempIDs[v.symbol] = unresolvedEntityID
				}
			}

		case string:
			// Assume the string is an ident name, and resolve it.
			ident, err := ResolveIdent(conn, v)
			if err != nil {
				return nil, err
			}
			assertion.entityID = ident.ID
		}
	}

	// Allocate ids for tempIDs.
	for symbol, id := range tempIDs {
		if id != unresolvedEntityID {
			// Was already set via db/id, db/ident, or unique attribute.
			continue
		}

		newID, err := conn.idManager.NextID()
		if err != nil {
			return nil, fmt.Errorf("allocating ID for tempID %q: %w", symbol, err)
		}
		tempIDs[symbol] = newID
	}

	// Second pass: Replace tempIDs with resolved IDs, and populate ResolvedAssertions.
	resolved := make([]ResolvedAssertion, len(assertions))
	for idx, assertion := range assertions {
		ra := ResolvedAssertion{
			Fact: Fact{
				Attribute: assertion.attribute.(ID),
				Tx:        tempIDs["txid"],
			},
			mode: assertion.mode,
		}

		switch v := assertion.entityID.(type) {
		case ID:
			ra.Fact.EntityID = v
		case tempID:
			ra.Fact.EntityID = tempIDs[v.symbol]
		default:
			panic(fmt.Sprintf("unhandled entityID type: %T", assertion.entityID))
		}

		if asTmpID, ok := assertion.value.(tempID); ok {
			ra.Value = tempIDs[asTmpID.symbol]
		} else {
			ra.Value = assertion.value
		}

		resolved[idx] = ra
	}

	// XXX: Get an actual database value. This should be used to determine the
	// basis of the
	db := Database{}
	return conn.assert(db, resolved, tempIDs)
}

func (conn *Connection) assert(db Database, assertions []ResolvedAssertion, resolvedIDs TempIDs) (*AssertResult, error) {
	err := conn.indexer.Write(assertions)
	if err != nil {
		return nil, fmt.Errorf("writing assertions: %w", err)
	}

	return &AssertResult{
		// XXX: Get db tx basis.
		DB:      db,
		Data:    assertions,
		TempIDs: resolvedIDs,
	}, nil
}

func (conn *Connection) GetEntity(idResolver Resolver) (Entity, error) {
	eid, err := idResolver.Resolve(conn)
	if err != nil {
		return Entity{}, fmt.Errorf("resolving entity ID: %w", err)
	}
	// TODO: cache entities
	ent := Entity{
		eid:   eid,
		state: make(map[ID]Value),
	}
	scan, err := conn.indexer.ScanEAVT(eid, nil)
	if err != nil {
		return ent, fmt.Errorf("scanning EAVT index: %v", err)
	}
	if err := scan.Produce(dataflow.NewContext(context.Background()), func(dc dataflow.DataflowCtx, fct *Fact) error {
		if fct == nil {
			return nil
		}
		attrEntity, err := conn.getSchemaEntity(fct.Attribute)
		if err != nil {
			return fmt.Errorf("fetching attribute schema: %w", err)
		}
		attrCardinality, err := attrEntity.Get(conn, IDCardinality)
		if err != nil {
			panic("error fetching attribute cardinality: " + err.Error())
		}

		val := fct.Value
		if attrCardinality == IDCardinalityMany {
			vals, ok := ent.state[fct.Attribute]
			if !ok {
				vals = make([]Value, 0, 1)
			}
			ent.state[fct.Attribute] = append(vals.([]Value), val)
		} else {
			ent.state[fct.Attribute] = val
		}
		if fct.Tx > ent.basisID {
			ent.basisID = fct.Tx
		}

		return nil
	}); err != nil {
		return ent, err
	}

	return ent, nil
}

// getSchemaEntity resolves a schema identity. This is a special case of
// GetEntity that assumes the argument passed in is an already-resolved ID
// pointing to a schema entity (attribute or ident). It also omits the attribute
// type lookup, since schema entities do not have db.cardinality/many (for now)
// attributes, looking up a schema entity for each attribute type would cause a
// recursive loop.
func (conn *Connection) getSchemaEntity(attrID ID) (Entity, error) {
	if ent, ok := conn.schemaEntityCache[attrID]; ok {
		return ent, nil
	}

	ent := Entity{
		eid:   attrID,
		state: make(map[ID]Value),
	}
	scan, err := conn.indexer.ScanEAVT(attrID, nil)
	if err != nil {
		return ent, fmt.Errorf("scanning EAVT index: %v", err)
	}
	if err := scan.Produce(dataflow.NewContext(context.Background()), func(dc dataflow.DataflowCtx, fct *Fact) error {
		if fct != nil {
			ent.state[fct.Attribute] = fct.Value
		}
		return nil
	}); err != nil {
		return ent, err
	}

	conn.schemaEntityCache[attrID] = ent

	return ent, nil
}
