package main

import "fmt"

func b() {
	v := 0
	a := func() int {
		v++
		return v
	}
	fmt.Printf("a type: %T, %v\n", a, a)
}

func main() {
	b()
}
