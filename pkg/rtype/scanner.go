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
	"fmt"
)

type tokenType int

const (
	ttIdent tokenType = iota
	ttString
	ttInteger
	ttDecimal

	// Keywords
	ttTrue
	ttFalse
	ttNull

	// Punctuation
	ttLBracket
	ttRBracket
	ttComma
	ttPipe
	ttEqual

	// Special
	ttEOF
	ttInvalid
)

type token struct {
	scn *scanner
	tokenType
	start, end int
}

func (t token) String() string {
	return string(t.scn.buf[t.start:t.end])
}

type scanner struct {
	buf      []byte
	i, start int
	err      error
}

func newScanner(in string) *scanner {
	return &scanner{
		buf: []byte(in),
	}
}

func (scn *scanner) next() (token, bool) {
	scn.chompWhitespace()
	scn.start = scn.i

	c, ok := scn.peek()
	if !ok {
		return scn.produceToken(ttEOF), false
	}
	scn.advance()

	switch c {
	case ',':
		return scn.produceToken(ttComma), true
	case '<':
		return scn.produceToken(ttLBracket), true
	case '>':
		return scn.produceToken(ttRBracket), true
	case '|':
		return scn.produceToken(ttPipe), true
	case '=':
		return scn.produceToken(ttEqual), true
	case '"':
		return scn.scanString()
	default:
		if c >= '0' && c <= '9' {
			return scn.scanNumber()
		}
		if c >= 'A' && c <= 'z' || c == '_' {
			return scn.scanIdentOrKeyword()
		}

		scn.err = fmt.Errorf("unexpected character: %s", []byte{c})
		return token{}, false
	}
}

func (scn *scanner) scanString() (token, bool) {
	for {
		c, ok := scn.peek()
		if !ok {
			scn.err = errors.New("unexpected EOF while scanning string")
			return scn.produceToken(ttEOF), false
		}

		if c == '"' {
			scn.advance()
			break
		}

		if c == '\\' {
			scn.advance()
			c, ok = scn.peek()
			if !ok {
				scn.err = errors.New("unexpected EOF in string escape sequence")
				return scn.produceToken(ttEOF), false
			}
			switch c {
			case '\\', '"':
				// OK
			default:
				scn.err = fmt.Errorf("invalid escape sequence at %d", scn.i)
				return scn.produceToken(ttInvalid), false
			}
		}
		scn.advance()
	}

	return scn.produceToken(ttString), true
}

func (scn *scanner) scanIdentOrKeyword() (token, bool) {
	for {
		c, ok := scn.peek()
		if !ok {
			break
		}
		if !(c >= 'A' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			break
		}
		scn.advance()
	}
	tok := scn.produceToken(ttIdent)
	switch tok.String() {
	case "true":
		tok.tokenType = ttTrue
	case "false":
		tok.tokenType = ttFalse
	case "null":
		tok.tokenType = ttNull
	}

	return tok, true
}

func (scn *scanner) scanNumber() (token, bool) {
	numType := ttInteger
loop:
	for {
		c, ok := scn.peek()
		if !ok {
			break
		}
		switch c {
		case '.':
			if numType == ttInteger {
				numType = ttDecimal
			} else {
				break
			}
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// OK = continue
		default:
			break loop
		}
		scn.advance()
	}
	return scn.produceToken(numType), true
}

func (scn *scanner) chompWhitespace() {
	for {
		c, ok := scn.peek()
		if !ok {
			return
		}
		switch c {
		case ' ', '\t', '\n':
			scn.advance()
		default:
			return
		}
	}
}

func (scn *scanner) expect(c byte) error {
	next, ok := scn.peek()
	if !ok {
		return errors.New("unexpected EOF")
	}
	if next != c {
		return fmt.Errorf("expected character %c but got %c", c, next)
	}
	scn.advance()
	return nil
}

func (scn *scanner) peek() (byte, bool) {
	if scn.i >= len(scn.buf) {
		return 0, false
	}
	return scn.buf[scn.i], true
}

func (scn *scanner) advance() {
	scn.i++
}

func (scn *scanner) produceToken(tt tokenType) token {
	t := token{
		scn:       scn,
		tokenType: tt,
		start:     scn.start,
		end:       scn.i,
	}
	scn.start = scn.i
	return t
}
