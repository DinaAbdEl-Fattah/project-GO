// main.go – Entry point for the Distributed Database system.
// Run each node in a SEPARATE terminal window using the commands in README.md.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"distributed-db/node"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	role := os.Args[1]
	args := os.Args[2:]

	switch role {

	// ─── Start as MASTER ────────────────────────────────────────
	case "master":
		// هنا تقدر تغير البورت الديفولت للـ master لو حابب تشغله على بورت تاني بدل 8080
		// You can change the default master port here if you want to run it on a different port instead of 8080.
		port := flag(args, "--port", "8080")
		slavesRaw := flag(args, "--slaves", "")
		var slaves []string
		if slavesRaw != "" {
			for _, s := range strings.Split(slavesRaw, ",") {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					slaves = append(slaves, trimmed)
				}
			}
		}

		m, err := node.NewMaster(port, slaves)
		if err != nil {
			log.Fatalf("Failed to create master: %v", err)
		}
		fmt.Printf("🟢 [MASTER] Starting on port %s\n", port)
		m.Start() // blocks forever

	// ─── Start as SLAVE ─────────────────────────────────────────
	case "slave":
		// هنا تقدر تغير إعدادات الـ slave الافتراضية زي البورت وعنوان الماستر وباقي الزملاء (peers)
		// Here you can change the default slave settings like port, master address, and peers.
		port := flag(args, "--port", "8081")
		slaveIP := flag(args, "--ip", "localhost")
		masterAddr := flag(args, "--master", "http://localhost:8080")
		peersRaw := flag(args, "--peers", "")
		priorityStr := flag(args, "--priority", "1")

		// --peers is a comma-separated list of sibling slave addresses
		var peers []string
		if peersRaw != "" {
			peers = strings.Split(peersRaw, ",")
		}

		priority, err := strconv.Atoi(priorityStr)
		if err != nil || priority < 1 {
			// هنا بنحدد الـ priority الافتراضية لو المستخدم مدخلش رقم صحيح (رقم 1 هو الأعلى)
			// Here we set the default priority if the user doesn't provide a valid number (1 is the highest).
			priority = 1
		}

		s, err := node.NewSlave(port, masterAddr, peers, priority, slaveIP)
		if err != nil {
			log.Fatalf("Failed to create slave: %v", err)
		}
		fmt.Printf("🔵 [SLAVE] Starting on port %s (priority %d, master %s)\n",
			port, priority, masterAddr)
		s.Start() // blocks forever

	// ─── Start as GUI Server ──────────────────────────────────────
	case "gui":
		port := flag(args, "--port", "3000")
		fmt.Printf("🎨 [GUI Dashboard] Starting on http://localhost:%s\n", port)
		log.Fatal(http.ListenAndServe(":"+port, http.FileServer(http.Dir("./dashboard"))))

	default:
		printUsage()
		os.Exit(1)
	}
}

// flag looks for --key value in args and returns value, or defaultVal if not found.
func flag(args []string, key, defaultVal string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == key {
			return args[i+1]
		}
	}
	return defaultVal
}

func printUsage() {
	fmt.Println(`
Distributed Database – Usage
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Start master node:
    go run main.go master --port 8080

  Start master with pre-defined slaves (optional):
    go run main.go master --port 8080 --slaves http://localhost:8081,http://localhost:8082,http://localhost:8083

  Start slave 1 (highest priority = promoted first on master failure):
    go run main.go slave --port 8081 --master http://localhost:8080 --priority 1

  Start slave 2:
    go run main.go slave --port 8082 --master http://localhost:8080 --priority 2

  Start slave 3:
    go run main.go slave --port 8083 --master http://localhost:8080 --priority 3

  Start slave on a remote machine (specify its own IP):
    go run main.go slave --port 8081 --master http://192.168.1.10:8080 --ip 192.168.1.11 --priority 1

  Start GUI dashboard:
    go run main.go gui --port 3000

Notes:
  - Peer discovery is AUTOMATIC. The master pushes the peer list to every slave upon registration.
  - The --peers flag is still accepted for backward compatibility but is no longer needed.
  - All slave operations require manual approval from the master (Dashboard or terminal: y <id> / n <id>).
  - If master fails, the slave with the lowest priority number is automatically promoted to master.
`)
}
