# Distributed Database System

This is a distributed database system built in Go, featuring a Master-Slave architecture as requested.

## Features implemented:
1. **1 Master, 3 Slaves**: The system allows multiple nodes to run on different ports.
2. **Master executes Writes**: All database modifications (Create, Insert, Update, Delete, etc.) are handled by the master.
3. **Simultaneous Broadcast (Goroutines)**: The master uses a separate goroutine for each slave to broadcast changes to all slaves simultaneously.
4. **Slaves are Read-Only**: Read queries (Select) on slaves are processed locally without contacting the master.
5. **Slaves Forward Writes**: If a write request is sent to a slave, the slave proxies it to the master.
6. **Fault Tolerance & Leader Election**: If the master dies, the slaves detect it through heartbeats. The slave with the highest priority (lowest number) automatically promotes itself to become the new master.

---

## 🚀 How to Run the Project (طريقة التشغيل)

You will need to open **4 separate terminal windows** (CMD or PowerShell) to run the 4 nodes at the same time.

### 1. Start the Master (Run this on the Master machine - IP: 172.20.10.2)
Open terminal 1 and run:
```bash
go run main.go master --port 8080
```
*Alternatively, you can pre-define your slave URLs using the `--slaves` flag:*
```bash
go run main.go master --port 8080 --slaves http://172.20.10.8:8081,http://172.20.10.9:8081,http://172.20.10.10:8081
```

### 2. Start Slave 1 (Run this on Slave 1 machine - IP: 172.20.10.8)
Open terminal 2 and run:
```bash
go run main.go slave --port 8081 --master http://172.20.10.2:8080 --ip 172.20.10.8 --priority 1
```

### 3. Start Slave 2 (Run this on Slave 2 machine - IP: 172.20.10.9)
Open terminal 3 and run:
```bash
go run main.go slave --port 8081 --master http://172.20.10.2:8080 --ip 172.20.10.9 --priority 2
```

### 4. Start Slave 3 (Run this on Slave 3 machine - IP: 172.20.10.10)
Open terminal 4 and run:
```bash
go run main.go slave --port 8081 --master http://172.20.10.2:8080 --ip 172.20.10.10 --priority 3
```

*(Note: Sibling peer discovery is now fully automatic. The master dynamically synchronizes and propagates peer lists to all slaves upon registration).*

---

## 🧪 How to Test (طريقة التجربة)

You can use Postman, Thunder Client, or `curl` commands to test the API.

### 1. Create a Database (Send to Master)
```bash
curl -X POST http://localhost:8080/db/create \
-H "Content-Type: application/json" \
-d "{\"db_name\": \"testdb\"}"
```

### 2. Create a Table (Send to Master)
```bash
curl -X POST http://localhost:8080/table/create \
-H "Content-Type: application/json" \
-d "{\"db_name\": \"testdb\", \"table_name\": \"users\", \"attributes\": [{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"name\", \"type\": \"string\"}]}"
```

### 3. Insert Data (Send to Master)
```bash
curl -X POST http://localhost:8080/record/insert \
-H "Content-Type: application/json" \
-d "{\"db_name\": \"testdb\", \"table_name\": \"users\", \"record\": {\"id\": 1, \"name\": \"Ahmed\"}}"
```

### 4. Read Data from a Slave (Read-Only)
```bash
curl -X POST http://localhost:8081/record/select \
-H "Content-Type: application/json" \
-d "{\"db_name\": \"testdb\", \"table_name\": \"users\", \"condition\": {}}"
```

### 5. Send a Write to a Slave (It will be forwarded to Master!)
```bash
curl -X POST http://localhost:8082/record/insert \
-H "Content-Type: application/json" \
-d "{\"db_name\": \"testdb\", \"table_name\": \"users\", \"record\": {\"id\": 2, \"name\": \"Ali\"}}"
```

### 6. Test Fault Tolerance (Leader Election)
1. Stop the Master node (Ctrl+C in terminal 1).
2. Look at the terminal for Slave 1 (Port 8081).
3. Wait 2-3 seconds. You will see a message: `👑 Promoting to MASTER`.
4. Now, Slave 1 is the new master! You can send write requests to port 8081.

---

## ⚙️ Architecture details (For the assignment report)
- **Goroutines**: HTTP requests are automatically handled in goroutines by Go's `net/http`. `master.go` uses goroutines to broadcast changes. `slave.go` uses goroutines for background heartbeats.
- **Simultaneous dispatch**: In `broadcastReplication()` inside `master.go`, a `sync.WaitGroup` ensures that requests to all slaves are fired exactly at the same time using `go func()`.
- **Fault Tolerance**: The system is highly available. As long as one node is alive, the database continues to function.

