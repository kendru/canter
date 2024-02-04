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
	"fmt"
	"strconv"
)

type booleanLiteral struct {
	val bool
}

func NewBooleanLiteral(val bool) *booleanLiteral {
	return &booleanLiteral{val}
}

func (t *booleanLiteral) TypeTag() string {
	if t.val {
		return "true"
	}
	return "false"
}

func (t *booleanLiteral) ParseString(in string) (any, error) {
	parsedVal, err := RTypeBool.Parse(in)
	if err != nil {
		return nil, err
	}
	if t.val != parsedVal.(bool) {
		return nil, ErrOutOfRange
	}

	return t.val, nil
}

func (t *booleanLiteral) concreteTypeMarker() {}

func (t *booleanLiteral) parentType() ConcreteType {
	return RTypeBool
}

type int64Literal struct {
	val int64
}

func NewInt64Literal(val int64) *int64Literal {
	return &int64Literal{val}
}

func (t *int64Literal) TypeTag() string {
	return strconv.FormatInt(t.val, 10)
}

func (t *int64Literal) ParseString(in string) (any, error) {
	parsedVal, err := RTypeInt64.Parse(in)
	if err != nil {
		return nil, err
	}
	if t.val != parsedVal.(int64) {
		return nil, ErrOutOfRange
	}

	return t.val, nil
}

func (t *int64Literal) concreteTypeMarker() {}

func (t *int64Literal) parentType() ConcreteType {
	return RTypeInt64
}

type float64Literal struct {
	val float64
}

func NewFloat64Literal(val float64) *float64Literal {
	return &float64Literal{val}
}

func (t *float64Literal) TypeTag() string {
	return strconv.FormatFloat(t.val, 'f', -1, 64)
}

func (t *float64Literal) ParseString(in string) (any, error) {
	parsedVal, err := RTypeFloat64.Parse(in)
	if err != nil {
		return nil, err
	}
	if t.val != parsedVal.(float64) {
		return nil, ErrOutOfRange
	}

	return t.val, nil
}

func (t *float64Literal) concreteTypeMarker() {}

func (t *float64Literal) parentType() ConcreteType {
	return RTypeFloat64
}

type stringLiteral struct {
	val string
}

func NewStringLiteral(val string) *stringLiteral {
	return &stringLiteral{val}
}

func (t *stringLiteral) TypeTag() string {
	return fmt.Sprintf("%q", t.val)
}

func (t *stringLiteral) ParseString(in string) (any, error) {
	parsedVal, err := RTypeString.Parse(in)
	if err != nil {
		return nil, err
	}
	if t.val != parsedVal.(string) {
		return nil, ErrOutOfRange
	}

	return t.val, nil
}

func (t *stringLiteral) concreteTypeMarker() {}

func (t *stringLiteral) parentType() ConcreteType {
	return RTypeString
}
