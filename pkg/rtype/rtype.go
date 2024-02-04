// The rtype package contains the Canter type system. This system provides a
// representation for typed values that can be parsed from input, converted
// between Go types and types within the database, and serialized for display.

// Concrete (Base / Parameterized) / Generic
// Scalar / Composite
// Builtin / User-Defined

// **Generic** type _instantiates_ to **Parameterized** type.
// **Base**  type constructed directly
// Both generic and base types can be registered.

package rtype

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNoInput          = errors.New("no input provided")
	ErrMalformed        = errors.New("input malformed")
	ErrOutOfRange       = errors.New("input out of range")
	ErrValidationFailed = errors.New("input validation failed")
)

// ConcreteType is a "useable" type. That is, it represents a BaseType or a
// parameterized type that has been instantiated from a generic type.
type ConcreteType interface {
	ValueParser
	TypeTag() string

	// Prevent external implementations.
	concreteTypeMarker()
}

type ValueParser interface {
	// ParseString parses an input string and produces a typed value or a parse
	// error.
	ParseString(in string) (any, error)
}

// BaseType is a ConcreteType that does not have any type parameters.
type BaseType struct {
	Tag   string
	Parse func(string) (any, error)
}

func (t BaseType) TypeTag() string {
	return t.Tag
}

func (t BaseType) ParseString(in string) (any, error) {
	return t.Parse(in)
}

func (t BaseType) concreteTypeMarker() {}

// NullType is a singleton type whose only value is `null`.
type nullType struct{}

func (t nullType) TypeTag() string {
	return "null"
}

func (t nullType) ParseString(in string) (any, error) {
	if in == "" {
		return nil, nil
	}
	return nil, errors.New("expected no input for null value")
}

func (t nullType) concreteTypeMarker() {}

// UnionType is a list of types that a particular value could take on.
type UnionType struct {
	Variants []ConcreteType
}

func (t UnionType) TypeTag() string {
	var sb strings.Builder
	for i, v := range t.Variants {
		if i > 0 {
			sb.WriteByte('|')
		}
		sb.WriteString(v.TypeTag())
	}
	return sb.String()
}

func (t UnionType) ParseString(in string) (any, error) {
	for _, v := range t.Variants {
		if val, err := v.ParseString(in); err == nil {
			return val, nil
		}
	}
	return nil, errors.Join(fmt.Errorf("not parsable by any variant: %s", t.TypeTag()), ErrOutOfRange)
}

func (t UnionType) concreteTypeMarker() {}

func NewUnionType(variants ...ConcreteType) *UnionType {
	return &UnionType{
		Variants: variants,
	}
}

// ParameterizedType is a ConcreteType that was constructed by a GenericType
// with specific parameters. This type is private because it is designed to only
// be constructed using generic types
type ParameterizedType struct {
	parent *GenericType
	params map[string]any
	ValueParser
}

func (t *ParameterizedType) TypeTag() string {
	return t.parent.Tag
}

func (t *ParameterizedType) GetParams() map[string]any {
	return t.params
}

func (t ParameterizedType) concreteTypeMarker() {}

type GenericType struct {
	// Tag is the tag used to register this type in a Registry. It must match
	// the Tag() of concrete types produced by calling Parameterize().
	Tag string

	// Instantiate gets a concrete type given a map of validated type
	// parameters that have been validated according to Parameters.
	Instantiate func(params map[string]any) (ValueParser, error)

	// Parameters describes the parameter that should be passed to Instantiate().
	Parameters []TypeParameter
}

type TypeParameter struct {
	Name         string
	Type         ConcreteType
	DefaultValue any
}

func InstantiateParameterized(g *GenericType, params map[string]any) (*ParameterizedType, error) {
	// TODO: The parsing code ensures that the params passed to
	// InstantiateParameterized conform to the type parameters, but there is no
	// check for constructing a parameterized type directly. We should ensure
	// that any public API for constructing types performs validation.
	vp, err := g.Instantiate(params)
	if err != nil {
		return nil, err
	}
	return &ParameterizedType{
		parent:      g,
		params:      params,
		ValueParser: vp,
	}, nil
}

// derivedType represents a type that is based on another type but provides
// alternate naming, semantics, etc.
type derivedType interface {
	parentType() ConcreteType
}

func RootType(t ConcreteType) ConcreteType {
	if asDerived, ok := t.(derivedType); ok {
		return RootType(asDerived.parentType())
	}
	return t
}

type aliasType struct {
	tag   string
	inner ConcreteType
}

func NewAliasType(tag string, inner ConcreteType) *aliasType {
	return &aliasType{
		tag:   tag,
		inner: inner,
	}
}

func (vt *aliasType) TypeTag() string {
	return vt.tag
}

func (vt *aliasType) ParseString(in string) (any, error) {
	return vt.inner.ParseString(in)
}

func (t aliasType) concreteTypeMarker() {}

func (vt *aliasType) parentType() ConcreteType {
	return vt.inner
}
