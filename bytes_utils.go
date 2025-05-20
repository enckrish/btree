package btree

import (
	"bytes"
	"fmt"
)

type Bytes = []byte

// Uses linear search to find the lower bound index, works better than binary search version in most cases
func lowerBoundBytesArr(arr []Bytes, key Bytes) (int, bool) {
	for i := range arr {
		cmp := bytes.Compare(arr[i], key)
		if cmp >= 0 {
			return i, cmp == 0
		}
	}
	return len(arr), false
}

func printBytesArrAsStr(bs []Bytes) {
	for _, b := range bs {
		fmt.Println(string(b))
	}
}
