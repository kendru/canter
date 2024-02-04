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
