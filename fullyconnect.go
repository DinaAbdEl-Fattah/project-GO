package main

import (
	"fmt"
	//"time"
)

func main() {
	fmt.Println("Main start")
	//go print_text("hello")

	//go goroutine("Go2")
	//goroutine("Go1")

	//time.Sleep(5 * time.Second)

	go func(msg string){
		fmt.Println(msg)
	}("Goroutine 1")

	func(msg string){
		fmt.Println(msg)
	}("Goroutine 2")

	func(msg string){
		fmt.Println(msg)
	}("Goroutine 3")

	fmt.Println("Main End")
}
/*func print_text(msg string){
	for i:=1;i<=5;i++{
	    fmt.Println(msg, ":", i)
	    time.Sleep(2 * time.Second)
	}
}*/

/*func goroutine(msg string){
	for i := 1; i <= 5 ; i++{
		time.Sleep(time.Second)
		fmt.Println(msg,":",i)
	}
}*/