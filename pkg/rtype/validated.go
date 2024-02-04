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
	"errors"

	"github.com/contomap/iri"
)

type validatedType struct {
	tag      string
	inner    ConcreteType
	validate func(any) error
}

var (
	RTypeIRI = NewValidatedType("iri", RTypeString, validateIRI)
	// regexp.MustCompile(`^\d*\.?\d*$`)
)

func NewValidatedType(tag string, inner ConcreteType, validate func(any) error) *validatedType {
	return &validatedType{
		tag:      tag,
		inner:    inner,
		validate: validate,
	}
}

func (vt *validatedType) TypeTag() string {
	return vt.tag
}

func (vt *validatedType) ParseString(in string) (any, error) {
	val, err := vt.inner.ParseString(in)
	if err != nil {
		return nil, err
	}

	if err := vt.validate(val); err != nil {
		return nil, errors.Join(err, ErrValidationFailed)
	}
	return val, nil
}

func (t *validatedType) concreteTypeMarker() {}

func (t *validatedType) parentType() ConcreteType {
	return t.inner
}

func validateIRI(val any) error {
	str := val.(string)
	if str == "" {
		return ErrNoInput
	}
	if _, err := iri.Parse(str); err != nil {
		return errors.Join(err, ErrMalformed)
	}
	return nil
}
