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
	"fmt"
)

type Assertable interface {
	// Assertions gets zero or more assertions to submit. This method is only
	// responsible for preparing data into zero or more Assertions, not
	// resolving any IDs. This will be done within the Connection while
	// preparing a transaction.
	Assertions(conn *Connection) ([]Assertion, error)
}

//go:generate stringer -type AssertMode -trimprefix AssertMode
type AssertMode uint8

const (
	AssertModeInvalid AssertMode = iota
	AssertModeAddition
	AssertModeRetraction
	AssertModeRedaction
)

// Assertion represents some action (add/retract/redact) and the unresolved
type Assertion struct {
	entityID  any
	attribute any
	value     any
	mode      AssertMode
	err       error
}

type ResolvedAssertion struct {
	Fact
	mode AssertMode
}

func (ra ResolvedAssertion) Mode() AssertMode {
	return ra.mode
}

// Assertions implements Assertable for Assertion.
// This method allows an assertion to be passed directly to
// *Connection.Assert().
func (a Assertion) Assertions(conn *Connection) ([]Assertion, error) {
	return []Assertion{a}, nil
}

func Assert(eid any, attribute any, value any) Assertion {
	add := Assertion{
		entityID:  eid,
		attribute: attribute,
		value:     value,
		mode:      AssertModeAddition,
	}
	add.checkAndSetErr()
	return add
}

func Retract(eid any, attribute any, value any) Assertion {
	add := Assertion{
		entityID:  eid,
		attribute: attribute,
		value:     value,
		mode:      AssertModeRetraction,
	}
	add.checkAndSetErr()
	return add
}

func Redact(eid any, attribute any, value any) Assertion {
	panic("Redact() not yet implemented")
}

// checkAndSetErr validates that the EntityID, Attribute, and Value of the
// assertion. If any are invalid, the `err` property is set
func (a *Assertion) checkAndSetErr() {
	a.err = errors.Join(
		guardMode(a.mode),
		guardEntityID(a.entityID),
		guardAttribute(a.attribute),
		guardValue(a.value),
	)
}

func guardMode(mode AssertMode) error {
	switch mode {
	case AssertModeAddition, AssertModeRetraction, AssertModeRedaction:
		return nil
	default:
		return fmt.Errorf("invalid mode for assertion: %q", mode)
	}
}

func guardEntityID(eid any) error {
	switch eid.(type) {
	case string, ID, tempID:
		return nil
	default:
		return fmt.Errorf("invalid type for entityID: %T", eid)
	}
}

func guardAttribute(attr any) error {
	switch attr.(type) {
	case string, Ident, ID:
		return nil
	default:
		return fmt.Errorf("invalid type for attribute: %T", attr)
	}
}

func guardValue(val any) error {
	return nil
}
