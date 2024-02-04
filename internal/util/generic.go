package util

func PtrTo[T any](v T) *T {
	return &v
}

func Map[T any, U any](xs []T, fn func(T) U) []U {
	out := make([]U, len(xs))
	for i, x := range xs {
		out[i] = fn(x)
	}
	return out
}

func MapFallible[T any, U any](xs []T, fn func(T) (U, error)) ([]U, error) {
	var err error
	out := make([]U, len(xs))
	for i, x := range xs {
		if out[i], err = fn(x); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func Filter[T any](xs []T, fn func(T) bool) []T {
	out := []T{}
	for _, x := range xs {
		if fn(x) {
			out = append(out, x)
		}
	}
	return out
}

func Keys[K comparable, V any](m map[K]V) []K {
	out := make([]K, len(m))
	i := 0
	for k := range m {
		out[i] = k
		i++
	}
	return out
}
