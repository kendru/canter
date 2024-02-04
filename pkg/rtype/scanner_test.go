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
