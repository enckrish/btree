package btree

import (
	"fmt"
)

type Hash = [32]byte
type Bytes = []byte

func ceilDiv(x, y int) int {
	return (x + y - 1) / y
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

func assertErr(cond bool, errs string, a ...any) {
	if !cond {
		fmt.Println(a...)
		panic(errs)
	}
}
