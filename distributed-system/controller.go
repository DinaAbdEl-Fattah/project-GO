package main

import (
	"fmt"
	"net"
	"sync"
)

func sendCommand(ip string, command string, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", ip+":9090")
	if err != nil {
		fmt.Println("Error connecting to", ip)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(command))
	if err != nil {
		fmt.Println("Error sending to", ip)
		return
	}

	fmt.Println("Sent to", ip)
}

func main() {
	devices := []string{
		 "127.0.0.1",
         "127.0.0.1",
         "127.0.0.1",
		 "192.168.137.119",
	}

	var command string
	fmt.Print("Enter command (lock / shutdown / wallpaper): ")
	fmt.Scanln(&command)

	var wg sync.WaitGroup

	for _, ip := range devices {
		wg.Add(1)
		go sendCommand(ip, command, &wg) // 🔥 concurrency
	}

	wg.Wait()
	fmt.Println("All commands sent!")
}