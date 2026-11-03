package main

import (
	"fmt"
	"math/rand"
	"time"
)

func worker(id int, jobs <-chan int, done chan<- bool) {
	for job := range jobs {

		sleepTime := rand.Intn(3) + 1
		time.Sleep(time.Duration(sleepTime) * time.Second)

		fmt.Printf("Worker %d processed job %d\n", id, job)
	}

	done <- true
}

func main() {

	rand.Seed(time.Now().UnixNano())

	jobs := make(chan int)
	done := make(chan bool)

	for i := 1; i <= 3; i++ {
		go worker(i, jobs, done)
	}

	for j := 1; j <= 6; j++ {
		jobs <- j
	}
	
	close(jobs)


	for i := 1; i <= 3; i++ {
		<-done
	}

	fmt.Println("All jobs completed")
}