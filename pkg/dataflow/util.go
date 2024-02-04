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
