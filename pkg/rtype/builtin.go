package rtype

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/gofrs/uuid/v5"
	"github.com/oklog/ulid/v2"
)

var (
	RTypeString = &BaseType{
		Tag: "string",
		Parse: func(in string) (any, error) {
			return strings.Clone(in), nil
		},
	}

	RTypeInt64 = &BaseType{
		Tag: "int64",
		Parse: func(in string) (any, error) {
			if in == "" {
				return nil, ErrNoInput
			}

			n, err := strconv.ParseInt(in, 10, 64)
			if err != nil {
				numErr := err.(*strconv.NumError)
				if errors.Is(numErr.Err, strconv.ErrSyntax) {
					return nil, ErrMalformed
				}
				if errors.Is(numErr.Err, strconv.ErrRange) {
					return nil, ErrOutOfRange
				}
				panic("unexpected error from strconv.ParseInt()")
			}
			return n, nil
		},
	}

	RTypeFloat64 = &BaseType{
		Tag: "float64",
		Parse: func(in string) (any, error) {
			if in == "" {
				return nil, ErrNoInput
			}

			n, err := strconv.ParseFloat(in, 64)
			if err != nil {
				numErr := err.(*strconv.NumError)
				if errors.Is(numErr.Err, strconv.ErrSyntax) {
					return nil, ErrMalformed
				}
				if errors.Is(numErr.Err, strconv.ErrRange) {
					return nil, ErrOutOfRange
				}
				panic("unexpected error from strconv.ParseInt()")
			}
			return n, nil
		},
	}

	RTypeBool = &BaseType{
		Tag: "boolean",
		Parse: func(in string) (any, error) {
			switch strings.ToLower(in) {
			case "":
				return nil, ErrNoInput
			case "true":
				return true, nil
			case "false":
				return false, nil
			default:
				return nil, fmt.Errorf("expected true|false: %w", ErrMalformed)
			}
		},
	}

	RTypeUUID = &BaseType{
		Tag: "uuid",
		Parse: func(in string) (any, error) {
			if in == "" {
				return nil, ErrNoInput
			}
			uuid, err := uuid.FromString(in)
			if err != nil {
				return nil, errors.Join(err, ErrMalformed)
			}
			return uuid, nil
		},
	}

	RTypeULID = &BaseType{
		Tag: "ulid",
		Parse: func(in string) (any, error) {
			if in == "" {
				return nil, ErrNoInput
			}
			ulid, err := ulid.Parse(in)
			if err != nil {
				return nil, errors.Join(err, ErrMalformed)
			}
			return ulid, nil
		},
	}

	RTypeType = &BaseType{
		Tag: "type",
		Parse: func(in string) (any, error) {
			if in == "" {
				return nil, ErrNoInput
			}
			return Parse(in)
		},
	}

	RTypeNull = nullType{}

	RTypeListGen = &GenericType{
		Tag: "list",
		Parameters: []TypeParameter{
			{
				Name: "elem",
				Type: RTypeType,
			},
		},
		Instantiate: func(params map[string]any) (ValueParser, error) {
			elem := params["elem"].(ConcreteType)
			return NewRTypeList(elem), nil
		},
	}

	RTypeDecimalGen = &GenericType{
		Tag: "decimal",
		Parameters: []TypeParameter{
			{
				Name: "precision",
				Type: RTypeInt64,
			},
			{
				Name:         "scale",
				Type:         RTypeInt64,
				DefaultValue: int64(0),
			},
		},
		Instantiate: func(params map[string]any) (ValueParser, error) {
			precision := params["precision"].(int64)
			scale := params["scale"].(int64)
			if precision < 0 || precision > math.MaxUint8 {
				return nil, fmt.Errorf("decimal.precision out of range (0, %d): %d", math.MaxUint8, precision)
			}

			if scale < 0 || scale > math.MaxUint8 {
				return nil, fmt.Errorf("decimal.scale out of range (0, %d): %d", math.MaxUint8, scale)
			}

			return &RTypeDecimal{
				precision: uint8(precision),
				scale:     uint8(scale),
			}, nil
		},
	}
)

// Parameterized types.

type RTypeList struct {
	elem ConcreteType
}

func NewRTypeList(elem ConcreteType) *RTypeList {
	return &RTypeList{
		elem: elem,
	}
}

func (t RTypeList) ParseString(in string) (any, error) {
	panic("TODO: Parse list")
}

type RTypeDecimal struct {
	precision, scale uint8
}

func (t RTypeDecimal) ParseString(in string) (any, error) {
	// Decimal is represented as a string.
	return in, nil
}
