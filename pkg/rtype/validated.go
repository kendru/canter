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
