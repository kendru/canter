package rtype

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

func TestBuiltinTypeTags(t *testing.T) {
	// Base types
	assert.Equal(t, "string", RTypeString.TypeTag())
	assert.Equal(t, "int64", RTypeInt64.TypeTag())
	assert.Equal(t, "float64", RTypeFloat64.TypeTag())
	assert.Equal(t, "boolean", RTypeBool.TypeTag())
	assert.Equal(t, "uuid", RTypeUUID.TypeTag())
	assert.Equal(t, "ulid", RTypeULID.TypeTag())
	assert.Equal(t, "iri", RTypeIRI.TypeTag())
	assert.Equal(t, "type", RTypeType.TypeTag())

	// Null type
	assert.Equal(t, "null", RTypeNull.TypeTag())

	// Parameterized types
	decimalInst, err := InstantiateParameterized(RTypeDecimalGen, map[string]any{
		"precision": int64(1),
		"scale":     int64(1),
	})
	assert.NoError(t, err)
	assert.Equal(t, "decimal", decimalInst.TypeTag())

	// Literal types
	assert.Equal(t, "true", NewBooleanLiteral(true).TypeTag())
	assert.Equal(t, "false", NewBooleanLiteral(false).TypeTag())
	assert.Equal(t, "123", NewInt64Literal(123).TypeTag())
	assert.Equal(t, "123.45", NewFloat64Literal(123.45).TypeTag())
	assert.Equal(t, `"Hello"`, NewStringLiteral("Hello").TypeTag())

	// Union types
	assert.Equal(t, "string|int64|null", NewUnionType(RTypeString, RTypeInt64, RTypeNull).TypeTag())
	assert.Equal(t, `"foo"|"bar"|42`, NewUnionType(NewStringLiteral("foo"), NewStringLiteral("bar"), NewInt64Literal(42)).TypeTag())

	//
}

func TestParseBuiltins(t *testing.T) {
	testCases := []struct {
		rtype    ConcreteType
		in       string
		expected any
		err      error
	}{
		// string
		{
			rtype:    RTypeString,
			in:       "",
			expected: "",
		},
		{
			rtype:    RTypeString,
			in:       "hello world",
			expected: "hello world",
		},
		// int64
		{
			rtype: RTypeInt64,
			in:    "",
			err:   ErrNoInput,
		},
		{
			rtype: RTypeInt64,
			in:    "hello",
			err:   ErrMalformed,
		},
		{
			rtype: RTypeInt64,
			in:    "123456789123456789123456789",
			err:   ErrOutOfRange,
		},
		{
			rtype:    RTypeInt64,
			in:       "0",
			expected: int64(0),
		},
		{
			rtype:    RTypeInt64,
			in:       "-9223372036854775808",
			expected: int64(-9223372036854775808),
		},
		{
			rtype:    RTypeInt64,
			in:       "9223372036854775807",
			expected: int64(9223372036854775807),
		},
		// float64
		{
			rtype: RTypeFloat64,
			in:    "",
			err:   ErrNoInput,
		},
		{
			rtype: RTypeFloat64,
			in:    "hello",
			err:   ErrMalformed,
		},
		{
			rtype: RTypeFloat64,
			in:    "1.797693134862315808e308",
			err:   ErrOutOfRange,
		},
		{
			rtype: RTypeFloat64,
			in:    "-1.797693134862315808e308",
			err:   ErrOutOfRange,
		},
		{
			rtype:    RTypeFloat64,
			in:       "0",
			expected: float64(0),
		},
		{
			rtype:    RTypeFloat64,
			in:       "1.7976931348623157e308",
			expected: float64(1.7976931348623157e308),
		},
		{
			rtype:    RTypeFloat64,
			in:       "5e-324",
			expected: float64(5e-324),
		},
		// bool
		{
			rtype: RTypeBool,
			in:    "",
			err:   ErrNoInput,
		},
		{
			rtype: RTypeBool,
			in:    "chicken",
			err:   ErrMalformed,
		},
		{
			rtype:    RTypeBool,
			in:       "true",
			expected: true,
		},
		{
			rtype:    RTypeBool,
			in:       "TrUe",
			expected: true,
		},
		{
			rtype:    RTypeBool,
			in:       "FALSE",
			expected: false,
		},
		// UUID
		{
			rtype: RTypeUUID,
			in:    "",
			err:   ErrNoInput,
		},
		{
			rtype:    RTypeUUID,
			in:       "4b189667-bfb0-4caf-8b43-91460e1abf89",
			expected: uuid.FromStringOrNil("4b189667-bfb0-4caf-8b43-91460e1abf89"),
		},
		{
			rtype:    RTypeUUID,
			in:       "00000000-0000-0000-0000-000000000000",
			expected: uuid.Nil,
		},
		{
			rtype: RTypeUUID,
			in:    "xyzzy",
			err:   ErrMalformed,
		},
		// ULID
		{
			rtype: RTypeULID,
			in:    "",
			err:   ErrNoInput,
		},
		{
			rtype:    RTypeULID,
			in:       "01G65Z755AFWAKHE12NY0CQ9FH",
			expected: ulid.MustParse("01G65Z755AFWAKHE12NY0CQ9FH"),
		},
		{
			rtype: RTypeULID,
			in:    "not a ulid",
			err:   ErrMalformed,
		},
		// IRI
		{
			rtype: RTypeIRI,
			in:    "",
			err:   ErrNoInput,
		},
		{
			rtype: RTypeIRI,
			in:    "not an IRI",
			err:   ErrMalformed,
		},
		{
			rtype:    RTypeIRI,
			in:       "https://www.example.com",
			expected: "https://www.example.com",
		},
		{
			rtype:    RTypeIRI,
			in:       "https://user:pass@example.com:123/foo/bar?name=Andrew#stuff",
			expected: "https://user:pass@example.com:123/foo/bar?name=Andrew#stuff",
		},
		// Null
		{
			rtype:    RTypeNull,
			in:       "",
			expected: nil,
		},
		// Type
		{
			rtype:    RTypeType,
			in:       "string",
			expected: RTypeString,
		},
		{
			rtype: RTypeType,
			// So meta. Much type.
			in:       "type",
			expected: RTypeType,
		},
		// Literals
		{
			rtype:    NewBooleanLiteral(true),
			in:       "true",
			expected: true,
		},
		{
			rtype: NewBooleanLiteral(false),
			in:    "true",
			err:   ErrOutOfRange,
		},
		{
			rtype: NewBooleanLiteral(false),
			in:    "42",
			err:   ErrMalformed,
		},
		{
			rtype:    NewInt64Literal(81235),
			in:       "81235",
			expected: int64(81235),
		},
		{
			rtype: NewInt64Literal(5280),
			in:    "999",
			err:   ErrOutOfRange,
		},
		{
			rtype: NewInt64Literal(42),
			in:    "true",
			err:   ErrMalformed,
		},
		{
			rtype:    NewFloat64Literal(543.21),
			in:       "543.21",
			expected: 543.21,
		},
		{
			rtype: NewFloat64Literal(123.45),
			in:    "88.77",
			err:   ErrOutOfRange,
		},
		{
			rtype: NewFloat64Literal(98.6),
			in:    "hello",
			err:   ErrMalformed,
		},
		{
			rtype:    NewStringLiteral("hello"),
			in:       "hello",
			expected: "hello",
		},
		{
			rtype: NewStringLiteral("ciao"),
			in:    "adios",
			err:   ErrOutOfRange,
		},
		// Union
		{
			rtype:    NewUnionType(RTypeInt64, RTypeFloat64),
			in:       "123",
			expected: int64(123),
		},
		{
			rtype:    NewUnionType(RTypeInt64, RTypeFloat64),
			in:       "123.45",
			expected: float64(123.45),
		},
		{
			rtype: NewUnionType(RTypeInt64, RTypeFloat64),
			in:    "hello",
			err:   ErrOutOfRange,
		},
		{
			rtype:    NewUnionType(NewStringLiteral("happy"), NewStringLiteral("sad")),
			in:       "happy",
			expected: "happy",
		},
		{
			rtype: NewUnionType(NewStringLiteral("happy"), NewStringLiteral("sad")),
			in:    "sanguine",
			err:   ErrOutOfRange,
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("%s:%q", tc.rtype.TypeTag(), tc.in), func(t *testing.T) {
			val, err := tc.rtype.ParseString(tc.in)
			if tc.err != nil {
				assert.ErrorIs(t, err, tc.err)
				assert.Nil(t, val)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, val)
			}
		})
	}
}

func TestValidatedType(t *testing.T) {
	onlyGreetings := NewValidatedType("greeting", RTypeString, func(str any) error {
		switch str.(string) {
		case "hello", "hi":
			return nil
		default:
			return errors.New("not a greeting")
		}
	})
	var err error

	_, err = onlyGreetings.ParseString("hi")
	assert.NoError(t, err)

	_, err = onlyGreetings.ParseString("goodbye")
	assert.ErrorIs(t, err, ErrValidationFailed)
}

func TestTypeRegistry(t *testing.T) {
	defer resetGlobal()
	tString, ok := Lookup("string")
	assert.True(t, ok, "should get primitive")
	assert.Equal(t, RTypeString, tString, "should get correct primitive")

	tMyString := NewAliasType("my_string", RTypeString)
	err := Register(tMyString)
	assert.NoError(t, err, "should not error registering type with new tag")

	err = Register(NewAliasType("my_string", RTypeString))
	assert.Error(t, err, "should error registering type with tag already registered")

	found, ok := Lookup("my_string")
	assert.True(t, ok, "should get registered type")
	assert.Equal(t, tMyString, found, "should get correct registered type")
}

func TestParseTypes(t *testing.T) {
	testCases := []struct {
		in       string
		expected ConcreteType
		err      error
	}{
		// Base types
		{
			in:       "string",
			expected: RTypeString,
		},
		{
			in:       "boolean",
			expected: RTypeBool,
		},
		{
			in:       "int64",
			expected: RTypeInt64,
		},
		{
			in:       "float64",
			expected: RTypeFloat64,
		},
		{
			in:       "iri",
			expected: RTypeIRI,
		},
		{
			in:       "ulid",
			expected: RTypeULID,
		},
		{
			in:       "uuid",
			expected: RTypeUUID,
		},
		{
			in:       "type",
			expected: RTypeType,
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			parsedType, err := Parse(tc.in)
			if tc.err != nil {
				assert.ErrorIs(t, err, tc.err)
				assert.Nil(t, parsedType)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, parsedType)
			}
		})
	}

	{
		pt, err := Parse(`decimal<precision = 6,scale =2>`)
		assert.NoError(t, err)
		assert.Equal(t, pt.TypeTag(), "decimal")
		asPT := pt.(*ParameterizedType)
		assert.Equal(t, RTypeDecimalGen, asPT.parent)
		assert.Equal(t, map[string]any{
			"precision": int64(6),
			"scale":     int64(2),
		}, asPT.GetParams())
	}
}

func TestEncodeType(t *testing.T) {
	assert.Equal(t, "string", Encode(RTypeString))

	decimalInst, err := InstantiateParameterized(RTypeDecimalGen, map[string]any{
		"precision": int64(6),
		"scale":     int64(2),
	})
	assert.NoError(t, err)
	assert.Equal(t, `decimal<precision = 6, scale = 2>`, Encode(decimalInst))
}
