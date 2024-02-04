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
