package btree

import (
	"fmt"
	"math"
)

type Bytes = []byte
type Hash = [32]byte

func ceilDiv(x, y int) int {
	return (x + y - 1) / y
}

func logBase(x, base int) float64 {
	r := math.Log2(float64(x)) / math.Log2(float64(base))
	return r
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func printStringBytes(bs []Bytes) {
	for _, b := range bs {
		fmt.Println(string(b))
	}
}

// hasRepeatsFn takes a list and a isEqual function and returns if a element occurs more than once in a row
// can be used to see if a list has all uniques, by sorting the list and then checking if hasRepeatsFn returns false
func hasRepeatsFn[T any](list []T, eq func(a, b T) bool) bool {
	if len(list) == 0 {
		return false
	}

	prev := list[0]
	for i, v := range list {
		if i == 0 {
			continue
		}
		if eq(v, prev) {
			return true
		}
		prev = v
	}
	return false
}

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	m := make(Set[T])
	return m
}

func (s Set[T]) Add(e T) (dup bool) {
	if s.Contains(e) {
		return true
	}

	s[e] = struct{}{}
	return false
}

func (s Set[T]) Remove(e T) {
	delete(s, e)
}

func (s Set[T]) Contains(e T) bool {
	_, ok := s[e]
	return ok
}

func (s Set[T]) Len() int {
	return len(s)
}

func assert(cond bool, f string, a ...any) {
	if cond {
		return
	}
	msg := fmt.Sprintf(f, a...)
	panic(msg)
}

func shiftElementsRight[T any](arr []T, from int, by int) ([]T, bool) {
	if cap(arr) < len(arr)+by {
		return arr, false
	}

	arr = arr[:len(arr)+by]
	copy(arr[from+by:], arr[from:])
	return arr, true
}

func shiftElementsLeft[T any](arr []T, from int, by int) ([]T, bool) {
	if from-by < 0 {
		return arr, false
	}

	copy(arr[from-by:], arr[from:])
	arr = arr[:len(arr)-by]
	return arr, true
}
