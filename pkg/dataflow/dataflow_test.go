package dataflow_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/kendru/canter/pkg/dataflow"
	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	ctx := dataflow.NewContext(context.Background())
	t.Run("empty", func(t *testing.T) {
		var res dataflow.SliceCollector[int]
		root := dataflow.NewMap[int, int](
			identity[int],
			res.Consume,
		)
		err := dataflow.SliceScanner[int]{}.Produce(ctx, root.Consume)
		assert.NoError(t, err)
		assert.Empty(t, res.Slice())
	})

	t.Run("single", func(t *testing.T) {
		var res dataflow.SliceCollector[int]
		root := dataflow.NewMap[int, int](
			double,
			res.Consume,
		)
		err := dataflow.SliceScanner[int]{[]int{3}}.Produce(ctx, root.Consume)
		assert.NoError(t, err)
		out := res.Slice()
		assert.Equal(t, []int{6}, derefAll(out))
	})

	t.Run("multiple", func(t *testing.T) {
		var res dataflow.SliceCollector[int]
		root := dataflow.NewMap[int, int](
			double,
			res.Consume,
		)
		err := dataflow.SliceScanner[int]{[]int{1, 2}}.Produce(ctx, root.Consume)
		assert.NoError(t, err)
		out := res.Slice()
		assert.Equal(t, []int{2, 4}, derefAll(out))
	})
}

func TestFilter(t *testing.T) {
	ctx := dataflow.NewContext(context.Background())
	t.Run("empty", func(t *testing.T) {
		var res dataflow.SliceCollector[int]
		root := dataflow.NewFilter[int](
			pPositiveNumber,
			res.Consume,
		)
		err := dataflow.SliceScanner[int]{}.Produce(ctx, root.Consume)
		assert.NoError(t, err)
		assert.Empty(t, res.Slice())
	})

	t.Run("single", func(t *testing.T) {
		var res dataflow.SliceCollector[int]
		root := dataflow.NewFilter[int](
			pPositiveNumber,
			res.Consume,
		)
		err := dataflow.SliceScanner[int]{[]int{-1}}.Produce(ctx, root.Consume)
		assert.NoError(t, err)
		assert.Empty(t, res.Slice())
	})

	t.Run("multiple", func(t *testing.T) {
		var res dataflow.SliceCollector[int]
		root := dataflow.NewFilter[int](
			pPositiveNumber,
			res.Consume,
		)
		err := dataflow.SliceScanner[int]{[]int{-1, 0, 1}}.Produce(ctx, root.Consume)
		assert.NoError(t, err)
		out := res.Slice()
		assert.Equal(t, []int{1}, derefAll(out))
	})
}

func TestPipeline(t *testing.T) {
	ctx := dataflow.NewContext(context.Background())
	var res dataflow.SliceCollector[int]
	root := dataflow.NewMap[int, int](
		double,
		res.Consume,
	)
	parseInt := dataflow.NewMap[string, int](
		func(s string) (int, error) {
			return strconv.Atoi(s)
		},
		root.Consume,
	)
	canParseNum := dataflow.NewFilter[string](
		func(s *string) bool {
			_, err := strconv.Atoi(*s)
			return err == nil
		},
		parseInt.Consume,
	)
	err := dataflow.SliceScanner[string]{[]string{"1", "2", "foo", "3"}}.Produce(ctx, canParseNum.Consume)
	assert.NoError(t, err)
	out := res.Slice()
	assert.Equal(t, []int{2, 4, 6}, derefAll(out))
}

func derefAll[T any](xs []*T) []T {
	out := make([]T, len(xs))
	for i := range xs {
		out[i] = *xs[i]
	}
	return out
}

func identity[T any](x T) (T, error) {
	return x, nil
}

func double(x int) (int, error) {
	return x * 2, nil
}

func pPositiveNumber(x *int) bool {
	return *x > 0
}
