package rtype

import "errors"

var globalReg = NewRegistry()

func init() {
	registerBuiltins()
}

type Registry struct {
	concrete map[string]ConcreteType
	generic  map[string]*GenericType
}

func NewRegistry() *Registry {
	return &Registry{
		concrete: make(map[string]ConcreteType),
		generic:  make(map[string]*GenericType),
	}
}

func (r *Registry) Lookup(tag string) (ConcreteType, bool) {
	rt, ok := r.concrete[tag]
	return rt, ok
}

func (r *Registry) Register(t ConcreteType) error {
	if _, ok := r.concrete[t.TypeTag()]; ok {
		return errors.New("type already registered")
	}
	r.concrete[t.TypeTag()] = t
	return nil
}

func (r *Registry) LookupGeneric(tag string) (*GenericType, bool) {
	rt, ok := r.generic[tag]
	return rt, ok
}

func (r *Registry) RegisterGeneric(t *GenericType) error {
	if _, ok := r.generic[t.Tag]; ok {
		return errors.New("generic type already registered")
	}
	r.generic[t.Tag] = t
	return nil
}

func Lookup(tag string) (ConcreteType, bool) {
	return globalReg.Lookup(tag)
}

func Register(t ConcreteType) error {
	return globalReg.Register(t)
}

func MustRegister(t ConcreteType) {
	if err := Register(t); err != nil {
		panic(err)
	}
}

func LookupGeneric(tag string) (*GenericType, bool) {
	return globalReg.LookupGeneric(tag)
}

func RegisterGeneric(t *GenericType) error {
	return globalReg.RegisterGeneric(t)
}

func MustRegisterGeneric(t *GenericType) {
	if err := RegisterGeneric(t); err != nil {
		panic(err)
	}
}

func resetGlobal() {
	for k := range globalReg.concrete {
		delete(globalReg.concrete, k)
	}
	for k := range globalReg.generic {
		delete(globalReg.generic, k)
	}
	registerBuiltins()
}

func registerBuiltins() {
	MustRegister(RTypeString)
	MustRegister(NewAliasType("text", RTypeString))
	MustRegister(RTypeInt64)
	MustRegister(RTypeFloat64)
	MustRegister(RTypeBool)
	MustRegister(RTypeIRI)
	MustRegister(RTypeULID)
	MustRegister(RTypeUUID)
	MustRegister(RTypeType)

	MustRegisterGeneric(RTypeListGen)
	MustRegisterGeneric(RTypeDecimalGen)
}
