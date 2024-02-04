package dataflow

import "errors"

type SliceScanner[T any] struct {
	Slice []T
}

// Produce implements Producer[T] for SliceScanner[T].
func (s SliceScanner[T]) Produce(ctx DataflowCtx, next ConsumeFn[T]) error {
	for i := range s.Slice {
		if err := next(ctx, &s.Slice[i]); err != nil {
			return err
		}
	}
	return next(ctx, nil)
}

type SliceCollector[T any] struct {
	xs       []*T
	consumed bool
}

func NewSliceCollector[T any](xs []*T) *SliceCollector[T] {
	return &SliceCollector[T]{xs: xs}
}

func (c *SliceCollector[T]) Consume(_ DataflowCtx, x *T) error {
	if c.consumed {
		return errors.New("sliceCollector already consumed")
	}
	if x == nil {
		return nil
	}
	c.xs = append(c.xs, x)
	return nil
}

func (c *SliceCollector[T]) Slice() []*T {
	xs := c.xs
	c.xs = nil
	c.consumed = true
	return xs
}

func CollectIntoSlice[T any](ctx DataflowCtx, p Producer[T]) ([]*T, error) {
	var res SliceCollector[T]
	err := p.Produce(ctx, res.Consume)
	return res.Slice(), err

}
