package main

import(
	"fmt"
	"time"
)

func main() {
	fmt.Println("main start")
	//go print_taxt("Hello")

	ch := make(chan int)  // create
	go func(){
		ch <- "hello world" // send
	}()

	msg := <- ch //resever

	fmt.Println(msg)
	go gorouten("Go1")
	go gorouten("Go2")

	time.Sleep(3*time.Second)  
	fmt.Println("main end")
}
/*func print_taxt(msg string){
	for i:=1;i<=5;i++{
		fmt.Println(msg , ":" ,i)
    	time.Sleep(2 * time.Second)  
	}
}*/

func gorouten(msg string){
	for i:=1;i<=5;i++ {
		time.Sleep(time.Second)
		fmt.Println(msg, ":" , i)
	}
}


/*func main() {
    ch := make(chan string)

    go func() {
        ch <- "Hello from goroutine"
    }()

    msg := <-ch
    fmt.Println(msg)
}*/

/*
package main

import (
    "fmt"
    "time"
)

func worker(id int) {
    fmt.Println("Worker", id, "started")
    time.Sleep(time.Second)
    fmt.Println("Worker", id, "finished")
}

func main() {
    for i := 1; i <= 3; i++ {
        go worker(i)
    }

    time.Sleep(2 * time.Second)
}*/