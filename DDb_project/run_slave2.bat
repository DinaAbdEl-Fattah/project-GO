@echo off
echo Starting Distributed DB Slave 2...
go run main.go slave --port 8081 --master http://172.20.10.2:8080 --ip 172.20.10.9 --priority 2
pause
