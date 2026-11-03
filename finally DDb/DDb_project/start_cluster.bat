@echo off
echo Starting Distributed DB Cluster...

echo Starting Master...
start cmd /k "go run main.go master --port 8080"

:: Wait 2 seconds for master to be ready
timeout /t 2 /nobreak >nul

echo Starting Slave 1...
start cmd /k "go run main.go slave --port 8081 --master http://localhost:8080 --priority 1"

echo Starting Slave 2...
start cmd /k "go run main.go slave --port 8082 --master http://localhost:8080 --priority 2"

echo Starting Slave 3...
start cmd /k "go run main.go slave --port 8083 --master http://localhost:8080 --priority 3"

echo Starting GUI Dashboard...
start cmd /k "go run main.go gui --port 3000"

echo Opening browser in 3 seconds...
timeout /t 3 /nobreak >nul
start http://localhost:3000

echo Done! You can close this window now.
