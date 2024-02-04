package rtype

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRoundtrips(t *testing.T) {
	testCases := []struct {
		name string
		str  string
	}{
		{
			name: "string literal",
			str:  `"my string"`,
		},
		{
			name: "integer literal",
			str:  `123`,
		},
		{
			name: "float literal",
			str:  `345.543`,
		},
		{
			name: "boolean literal",
			str:  `true`,
		},
		{
			name: "null",
			str:  `null`,
		},
		{
			name: "builtin primitive",
			str:  `string`,
		},
		{
			name: "primitive union",
			str:  `string|int64`,
		},
		{
			name: "mixed union",
			str:  `"test"|int64|true|12.34`,
		},
		// {
		// 	name: "parameterized type - named parameters",
		// 	str:  `decimal<precision = 10, scale=3>`,
		// },
		// {
		// 	name: "parameterized type - positional parameters",
		// 	str:  `decimal<10, 3>`,
		// },
		// {
		// 	name: "parameterized type - mixed named/positional parameters",
		// 	str:  `decimal<10, scale = 3>`,
		// },
		// {
		// 	name: "parameterized type - missing optional parameters",
		// 	str:  `decimal<10>`,
		// },
	}

	for _, tc := range testCases {
		p := newParser(tc.str)
		ct, err := p.parse()
		if !assert.NoError(t, err, "error parsing %s", tc.name) {
			return
		}
		assert.Equal(t, tc.str, ct.TypeTag(), "roundtrip failed for %s", tc.name)
	}
}

func TestParseParameterized(t *testing.T) {
	// For this test, we are using various combinations of parameterizing the
	// decimal generic type. This allows us to try multiple methods of
	// parameterization.
	testCases := []struct {
		name              string
		in                string
		expectedPrecision int64
		expectedScale     int64
		shouldErr         bool
	}{
		{
			name:              "positional - fully specified",
			in:                "decimal<9, 3>",
			expectedPrecision: 9,
			expectedScale:     3,
		},
		{
			name:              "positional - required specified, optional empty",
			in:                "decimal<7>",
			expectedPrecision: 7,
			expectedScale:     0,
		},
		{
			name:              "named - fully specified",
			in:                "decimal<precision = 10, scale=3>",
			expectedPrecision: 10,
			expectedScale:     3,
		},
		{
			name:              "named - required specified, optional empty",
			in:                "decimal<precision = 4>",
			expectedPrecision: 4,
			expectedScale:     0,
		},
		{
			name:              "mixed positional and named",
			in:                "decimal<8, scale = 2>",
			expectedPrecision: 8,
			expectedScale:     2,
		},
		{
			name:      "error - no parameter list",
			in:        "decimal",
			shouldErr: true,
		},
		{
			name:      "error - no parameter list",
			in:        "decimal<>",
			shouldErr: true,
		},
		{
			name:      "error - empty parameter",
			in:        "decimal<,4>",
			shouldErr: true,
		},
		{
			name:      "error - missing required parameter",
			in:        "decimal<scale=12>",
			shouldErr: true,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			p := newParser(testCase.in)
			ct, err := p.parse()
			if testCase.shouldErr {
				assert.Error(t, err)
				return
			}

			if !assert.NoError(t, err) {
				return
			}
			if !assert.Equal(t, "decimal", ct.TypeTag()) {
				return
			}

			pt := ct.(*ParameterizedType)
			assert.Equal(t, map[string]any{
				"precision": testCase.expectedPrecision,
				"scale":     testCase.expectedScale,
			}, pt.GetParams())
		})
	}
}
