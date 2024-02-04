package store

import "github.com/kendru/canter/pkg/dataflow"

type Indexer interface {
	Write([]ResolvedAssertion) error
	ScanEAVT(entityID ID, attribute *ID) (dataflow.Producer[Fact], error)
	ScanAEVT(attribute ID, entityID *ID) (dataflow.Producer[Fact], error)
	ScanAVET(attribute ID, val Value) (dataflow.Producer[Fact], error)
	ScanVAET(val Value, attribute *ID) (dataflow.Producer[Fact], error)
}
