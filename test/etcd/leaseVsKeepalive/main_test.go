package main

import (
	"fmt"
	"testing"
	"time"
)

func Test1(t *testing.T) {
	c := make(chan int)
	fmt.Printf("%#v\n", c)
	go func() {
		for i := 0; i < 10; i++ {
			select {
			case i := <-c:
				fmt.Printf("%#v\n", i)
			}
		}
	}()
	time.Sleep(2 * time.Second)
	close(c)
	//fmt.Printf("%#v\n", c)
}

func Test2(t *testing.T) {
	b := []byte("content-type: application/json")
	fmt.Printf("%b\n%x\n", b, b)
}
