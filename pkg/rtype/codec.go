package rtype

import (
	"bytes"
	"fmt"
)

func Parse(typeStr string) (ConcreteType, error) {
	return newParser(typeStr).parse()
}

func MustParse(typeStr string) ConcreteType {
	t, err := Parse(typeStr)
	if err != nil {
		panic(err)
	}
	return t
}

func Encode(ct ConcreteType) string {
	tag := ct.TypeTag()
	switch exactType := ct.(type) {
	case *ParameterizedType:
		var buf bytes.Buffer
		buf.WriteString(tag)
		buf.WriteByte('<')
		formalParams := exactType.parent.Parameters
		actualParams := exactType.params
		var hasWritten bool
		for _, formalParam := range formalParams {
			paramName := formalParam.Name
			actualParam, ok := actualParams[paramName]
			if !ok && formalParam.DefaultValue != nil {
				continue
			}
			if hasWritten {
				buf.WriteString(", ")
			}
			buf.WriteString(paramName)
			buf.WriteString(" = ")

			rootTypeTag := RootType(formalParam.Type).TypeTag()
			switch rootTypeTag {
			case "int64", "float64", "boolean":
				fmt.Fprintf(&buf, "%v", actualParam)
			case "string", "uuid", "ulid":
				fmt.Fprintf(&buf, "%q", actualParam)
			case "type":
				encodedType := Encode(actualParam.(ConcreteType))
				buf.WriteString(encodedType)
			default:
				panic("TODO: encode value from type " + rootTypeTag)
			}
			hasWritten = true
		}
		buf.WriteByte('>')
		return buf.String()

	default:
		return tag
	}
}
