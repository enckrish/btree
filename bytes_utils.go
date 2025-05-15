package btree

import (
	"bytes"
	"fmt"
	"sort"
)

type Bytes = []byte

var lowerBoundBytesArr func([]Bytes, Bytes) (int, bool) = lbLinSearch

func lbBinSearch(arr []Bytes, key Bytes) (int, bool) {
	return sort.Find(len(arr), func(i int) int {
		return bytes.Compare(key, arr[i])
	})
}

// Uses linear search to find the lower bound index, works better than binary search version in most cases
func lbLinSearch(arr []Bytes, key Bytes) (int, bool) {
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
