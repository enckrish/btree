package btree

import (
	"fmt"
	"math"
)

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

func assert(cond bool, f string, a ...any) {
	if cond {
		return
	}
	msg := fmt.Sprintf(f, a...)
	panic(msg)
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

func shlArr[T any](arr []T, by int) {
	assert(by >= 0, "cannot be shifted by negative number")
	if by >= len(arr) {
		return
	}
	copy(arr, arr[by:])
}

func shrArr[T any](arr []T, by int) {
	assert(by >= 0, "cannot be shifted by negative number")
	if by >= len(arr) {
		return
	}
	copy(arr[by:], arr)
}
