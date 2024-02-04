package rtype

import (
	"errors"
	"fmt"
	"strconv"
)

type parser struct {
	scn    *scanner
	peeked *token
}

func newParser(buf string) *parser {
	return &parser{
		scn: newScanner(buf),
	}
}

func (p *parser) parse() (ConcreteType, error) {
	return p.parseUnion()
}

func (p *parser) parseUnion() (ConcreteType, error) {
	variant, err := p.parseBase()
	if err != nil {
		return nil, err
	}
	variants := []ConcreteType{variant}
	for {
		if !p.nextTokenIs(ttPipe) {
			break
		}
		p.nextToken()

		nextVariant, err := p.parseBase()
		if err != nil {
			return nil, err
		}
		variants = append(variants, nextVariant)
	}
	if len(variants) == 1 {
		return variants[0], nil
	}

	return NewUnionType(variants...), nil
}

func (p *parser) parseBase() (ConcreteType, error) {
	tok, ok := p.nextToken()
	if !ok {
		return nil, p.scn.err
	}
	switch tok.tokenType {
	case ttString:
		// TODO: parse string
		stringSource := tok.String()
		val := stringSource[1 : len(stringSource)-1]
		return NewStringLiteral(val), nil
	case ttInteger:
		val, err := strconv.ParseInt(tok.String(), 10, 64)
		if err != nil {
			return nil, err
		}
		return NewInt64Literal(val), nil
	case ttDecimal:
		val, err := strconv.ParseFloat(tok.String(), 64)
		if err != nil {
			return nil, err
		}
		return NewFloat64Literal(val), nil
	case ttTrue:
		return NewBooleanLiteral(true), nil
	case ttFalse:
		return NewBooleanLiteral(false), nil
	case ttNull:
		return RTypeNull, nil
	case ttIdent:
		tag := tok.String()
		if p.nextTokenIs(ttLBracket) {
			gt, ok := LookupGeneric(tag)
			if !ok {
				return nil, fmt.Errorf("generic type %s not found", tag)
			}
			p.nextToken()
			params, err := p.parseParamList(gt)
			if err != nil {
				return nil, err
			}
			return InstantiateParameterized(gt, params)
		}
		ct, ok := Lookup(tag)
		if !ok {
			return nil, fmt.Errorf("type %s not found", tag)
		}
		return ct, nil
	default:
		return nil, fmt.Errorf("unexpected token: %s", tok)
	}
}

func (p *parser) parseParamList(t *GenericType) (map[string]any, error) {
	params := make(map[string]any)
	var i int
	for {
		if i >= len(t.Parameters) {
			return nil, fmt.Errorf("too many parameters parsing list for %s. expected %d but got more", t.Tag, len(t.Parameters))
		}

		tok, ok := p.peek()
		if !ok {
			return nil, errors.New("unclosed type parameters")
		}

		if tok.tokenType == ttComma {
			i++
			p.nextToken()
			continue
		}

		if tok.tokenType == ttRBracket {
			p.nextToken()
			break
		}

		var param = t.Parameters[i]
		if tok.tokenType == ttIdent {
			ident := tok.String()
			p.nextToken()
			if tok, _ := p.peek(); tok.tokenType == ttEqual {
				// If the next token is an equal sign, then this is a named
				// parameter assignment.
				var found bool
				for _, genericParam := range t.Parameters {
					if genericParam.Name == ident {
						param = genericParam
						found = true
						break
					}
				}
				if !found {
					return nil, fmt.Errorf("unknown type parameter %s.%s", t.Tag, ident)
				}

				// Skip the equal sign and advance tok to the start of the value.
				_, _ = p.nextToken()
			}
		}

		// FIXME:
		// What we actually want here is a parseParameter() that will
		// return a value appropriate to the parameter type. Instead,
		// since we know that literals will be parsed as literal types,
		// we parse the parameter as a type and extract the underlying
		// value as appropriate for the parameter type.
		var paramVal any
		valAsType, err := p.parseUnion()
		if err != nil {
			return nil, err
		}
		switch RootType(param.Type).TypeTag() {
		case "int64", "float64", "boolean":
			if paramVal, err = param.Type.ParseString(valAsType.TypeTag()); err != nil {
				return nil, err
			}
		case "string", "uuid", "uri":
			str := valAsType.TypeTag()
			if paramVal, err = param.Type.ParseString(str[1 : len(str)-1]); err != nil {
				return nil, err
			}
		default:
			panic(fmt.Errorf("TODO: implement non-literal parameter parsing! %T", t))
		}

		params[param.Name] = paramVal
	}

	for _, param := range t.Parameters {
		if _, ok := params[param.Name]; !ok {
			if param.DefaultValue == nil {
				return nil, fmt.Errorf("missing required type parameter %s.%s", t.Tag, param.Name)
			}
			params[param.Name] = param.DefaultValue
		}
	}

	return params, nil
}

func (p *parser) nextTokenIs(tt tokenType) bool {
	next, ok := p.peek()
	if !ok {
		return false
	}
	return next.tokenType == tt
}

func (p *parser) nextToken() (next token, ok bool) {
	if p.peeked != nil {
		next = *p.peeked
		ok = true
		p.peeked = nil
	} else {
		next, ok = p.scn.next()
	}

	return next, ok
}

func (p *parser) peek() (token, bool) {
	if p.peeked == nil {
		next, ok := p.scn.next()
		if !ok {
			return token{}, false
		}
		p.peeked = &next
	}
	return *p.peeked, true
}
