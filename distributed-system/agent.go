package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1024)

	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Read error:", err)
		return
	}

	message := strings.TrimSpace(string(buffer[:n]))
	fmt.Println("Received:", message)

	switch message {

	case "lock":
		fmt.Println("Locking device...")
		exec.Command("rundll32.exe", "user32.dll,LockWorkStation").Run()

	case "shutdown":
		fmt.Println("Shutting down...")
		exec.Command("shutdown", "/s", "/t", "0").Run()

	case "wallpaper":
		fmt.Println("Changing wallpaper...")

		// مسار الفولدر الحالي
		currentDir, _ := os.Getwd()

		// اسم الصورة ثابت
		imagePath := filepath.Join(currentDir, "wallpaper.jpg")

		fmt.Println("Path:", imagePath)

		// تأكد إن الصورة موجودة
		if _, err := os.Stat(imagePath); os.IsNotExist(err) {
			fmt.Println("Image not found!")
			return
		}

		// PowerShell + Windows API
		cmd := exec.Command("powershell", "-command",
			`Add-Type -TypeDefinition @"
using System.Runtime.InteropServices;
public class Wallpaper {
	[DllImport("user32.dll", SetLastError = true)]
	public static extern bool SystemParametersInfo(int uAction, int uParam, string lpvParam, int fuWinIni);
}
"@;
[Wallpaper]::SystemParametersInfo(20, 0, "`+imagePath+`", 3)`)

		err := cmd.Run()
		if err != nil {
			fmt.Println("Error:", err)
		}

	default:
		fmt.Println("Unknown command")
	}
}

func main() {
	listener, err := net.Listen("tcp", ":9090")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Agent running on port 9090...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn)
	}
}