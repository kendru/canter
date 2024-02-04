package dataflow

import (
	"context"
	"errors"
)

var ErrStop = errors.New("stop iteration")

// DataflowCtx is a context that is passed to each operator. It is a wrapper
// around the standard context.Context that allows us to augment the context
// with additional functionality in the future.
type DataflowCtx struct {
	context.Context
}

func NewContext(ctx context.Context) DataflowCtx {
	return DataflowCtx{ctx}
}

// Source is the interface for a data producer.
// It defines a single method, Produce, which is used to produce
// values and hand them off to the next operator in the dataflow.
type Producer[T any] interface {
	Produce(DataflowCtx, ConsumeFn[T]) error
}

type ConsumeFn[T any] func(DataflowCtx, *T) error

// Map is an Operator that applies a function to each input value and produces the result.
type Map[TIn any, TOut any] struct {
	mapper func(TIn) (TOut, error)
	next   ConsumeFn[TOut]
}

// NewMap creates a new Map operator.
func NewMap[TIn any, TOut any](
	mapper func(TIn) (TOut, error),
	next ConsumeFn[TOut],
) Map[TIn, TOut] {
	return Map[TIn, TOut]{
		mapper: mapper,
		next:   next,
	}
}

func (m *Map[TIn, TOut]) Consume(ctx DataflowCtx, in *TIn) error {
	if in == nil {
		return m.next(ctx, nil)
	}

	out, err := m.mapper(*in)
	if err != nil {
		return err
	}
	return m.next(ctx, &out)
}

// Filter is an Operator that filters input values based on a predicate.
type Filter[T any] struct {
	pred func(*T) bool
	next ConsumeFn[T]
}

// NewFilter creates a new Filter operator.
func NewFilter[T any](
	pred func(*T) bool,
	next ConsumeFn[T],
) Filter[T] {
	return Filter[T]{
		pred: pred,
		next: next,
	}
}

func (f Filter[T]) Consume(ctx DataflowCtx, item *T) error {
	if item == nil {
		return f.next(ctx, nil)
	}

	if f.pred(item) {
		return f.next(ctx, item)
	}
	return nil
}
