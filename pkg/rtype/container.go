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

package rtype

import (
	"database/sql"
	"database/sql/driver"
	"encoding"
	"fmt"
)

// RTypeContainer is a concrete type that wraps the ConcreteType interface type
// to provide a concrete type that implements a number of interfaces for
// marshaling to and from other representations.
type RTypeContainer struct {
	ConcreteType
}

func (rt *RTypeContainer) parentType() ConcreteType {
	return rt.ConcreteType
}

var _ encoding.TextUnmarshaler = (*RTypeContainer)(nil)
var _ encoding.TextMarshaler = (*RTypeContainer)(nil)
var _ fmt.Stringer = (*RTypeContainer)(nil)
var _ sql.Scanner = (*RTypeContainer)(nil)
var _ driver.Valuer = (*RTypeContainer)(nil)

func (rt RTypeContainer) MarshalText() ([]byte, error) {
	return []byte(rt.String()), nil
}

func (rt *RTypeContainer) UnmarshalText(data []byte) error {
	var err error
	rt.ConcreteType, err = Parse(string(data))
	return err
}

func (rt *RTypeContainer) String() string {
	return Encode(rt.ConcreteType)
}

func (rt *RTypeContainer) Scan(src any) error {
	switch val := src.(type) {
	case string:
		return rt.UnmarshalText([]byte(val))
	case []byte:
		return rt.UnmarshalText(val)
	default:
		return fmt.Errorf("cannot scan *RTypeContainer from %T", src)
	}
}

func (rt *RTypeContainer) Value() (driver.Value, error) {
	return rt.String(), nil
}

func RTypeContainerFrom(ct ConcreteType) *RTypeContainer {
	if asContainer, ok := ct.(*RTypeContainer); ok {
		return asContainer
	}

	return &RTypeContainer{
		ConcreteType: ct,
	}
}

func Unwrap(ct ConcreteType) ConcreteType {
	if asContainer, ok := ct.(*RTypeContainer); ok {
		return Unwrap(asContainer.ConcreteType)
	}
	return ct
}

func MustNewRTypeContainer(str string) RTypeContainer {
	out := RTypeContainer{}
	if err := out.UnmarshalText([]byte(str)); err != nil {
		panic(err)
	}
	return out
}
