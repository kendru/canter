package store

// Tx is is a transaction entity. Transaction entities are normal entities, but
// they are associated with a database value as of a particular point in time,
// and they themselves are not associated with any other transaction.
type Tx struct {
	eid   ID
	time  uint64
	state map[ID][]Value
}

// ID returns the entity ID associated with the transaction.
func (t Tx) ID() ID {
	return t.eid
}

func (t Tx) Time() uint64 {
	return t.time
}

// Database
type Database struct {
	Basis Tx
}
