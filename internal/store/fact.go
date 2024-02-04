package store

type Fact struct {
	EntityID  ID
	Attribute ID
	Value     Value
	Tx        ID
}
