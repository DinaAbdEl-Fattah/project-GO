package main

import (
	"bytes"
	"distributed-db/shared"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

var db shared.Database

var slaveNodes = []string{
	"http://localhost:8001",
	"http://localhost:8002",
}

func createDB(w http.ResponseWriter, r *http.Request) {
	db = shared.Database{
		Name:   "DistributedDB",
		Tables: make(map[string]*shared.Table),
	}

	fmt.Fprintln(w, "Database Created")
}

func createTable(w http.ResponseWriter, r *http.Request) {
	tableName := r.URL.Query().Get("name")

	db.Tables[tableName] = &shared.Table{
		Name:    tableName,
		Records: []shared.Record{},
	}

	replicate("/createTable?name=" + tableName)

	fmt.Fprintln(w, "Table Created")
}

func insertRecord(w http.ResponseWriter, r *http.Request) {
	tableName := r.URL.Query().Get("table")

	body, _ := io.ReadAll(r.Body)

	var record shared.Record

	json.Unmarshal(body, &record)

	table := db.Tables[tableName]

	table.Records = append(table.Records, record)

	replicateWithBody("/insert?table="+tableName, body)

	fmt.Fprintln(w, "Inserted")
}

func replicate(path string) {
	for _, node := range slaveNodes {
		http.Get(node + path)
	}
}

func replicateWithBody(path string, body []byte) {
	for _, node := range slaveNodes {
		http.Post(node+path, "application/json", bytes.NewBuffer(body))
	}
}

func main() {
	http.HandleFunc("/createDB", createDB)
	http.HandleFunc("/createTable", createTable)
	http.HandleFunc("/insert", insertRecord)

	fmt.Println("Master running on port 8000")

	http.ListenAndServe(":8000", nil)
}