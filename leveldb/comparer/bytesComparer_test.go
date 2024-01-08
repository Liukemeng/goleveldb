/*
 Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

 SPDX-License-Identifier: Apache-2.0
*/

package comparer

import (
	"fmt"
	"testing"
)

func TestSeparator(t *testing.T) {
	bc := new(bytesComparer)

	dst := make([]byte, 0)
	a := []byte("ock")
	b := []byte("duck")
	res := bc.Separator(dst, a, b)
	fmt.Printf("dst:%s,res:%s\n", dst, res)

	d := []byte("3333")
	e := changBytes(d)
	fmt.Printf("d:%s,e:%s\n", d, e)

	f := []byte("3333")
	g := f[:0]
	fmt.Printf("f:%s,g:%s\n", f, g)

}

func changBytes(b []byte) []byte {
	b[0] = 1
	b = append(b, 1)
	return b
}
