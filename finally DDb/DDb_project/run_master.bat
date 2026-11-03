@echo off
echo Starting Distributed DB Master...
go run main.go master --port 8080 --slaves http://172.20.10.8:8081,http://172.20.10.9:8081,http://172.20.10.10:8081
pause
