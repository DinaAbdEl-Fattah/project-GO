@echo off
echo Starting Distributed DB Slave 1...
go run main.go slave --port 8081 --master http://172.20.10.2:8080 --ip 172.20.10.8 --priority 1
pause
