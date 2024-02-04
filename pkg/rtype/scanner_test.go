package rtype

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScanner(t *testing.T) {
	scn := newScanner(`    "hello"	12 12.34 ,<>|= true false null my_ident`)
	expectedSequence := []struct {
		tokenType
		expectedString string
	}{
		{
			tokenType:      ttString,
			expectedString: `"hello"`,
		},
		{
			tokenType:      ttInteger,
			expectedString: "12",
		},
		{
			tokenType:      ttDecimal,
			expectedString: "12.34",
		},
		{
			tokenType:      ttComma,
			expectedString: ",",
		},
		{
			tokenType:      ttLBracket,
			expectedString: "<",
		},
		{
			tokenType:      ttRBracket,
			expectedString: ">",
		},
		{
			tokenType:      ttPipe,
			expectedString: "|",
		},
		{
			tokenType:      ttEqual,
			expectedString: "=",
		},
		{
			tokenType:      ttTrue,
			expectedString: "true",
		},
		{
			tokenType:      ttFalse,
			expectedString: "false",
		},
		{
			tokenType:      ttNull,
			expectedString: "null",
		},
		{
			tokenType:      ttIdent,
			expectedString: "my_ident",
		},
		{
			tokenType: ttEOF,
		},
	}
	for _, nextExpected := range expectedSequence {
		tok, ok := scn.next()
		assert.NoError(t, scn.err)
		if nextExpected.tokenType == ttEOF {
			assert.False(t, ok, "expected scanner to be done at EOF")
		} else {
			assert.True(t, ok, "scanner halted prematurely")
		}
		assert.Equal(t, nextExpected.tokenType, tok.tokenType)
		assert.Equal(t, nextExpected.expectedString, tok.String())
	}
}
