package main

import (
	"distributed-db/shared"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

var db shared.Database

func createTable(w http.ResponseWriter, r *http.Request) {
	tableName := r.URL.Query().Get("name")

	if db.Tables == nil {
		db.Tables = make(map[string]*shared.Table)
	}

	db.Tables[tableName] = &shared.Table{
		Name:    tableName,
		Records: []shared.Record{},
	}

	fmt.Fprintln(w, "Replicated Table")
}

func insertRecord(w http.ResponseWriter, r *http.Request) {
	tableName := r.URL.Query().Get("table")

	body, _ := io.ReadAll(r.Body)

	var record shared.Record

	json.Unmarshal(body, &record)

	table := db.Tables[tableName]

	table.Records = append(table.Records, record)

	fmt.Fprintln(w, "Replicated Insert")
}

func main() {
	http.HandleFunc("/createTable", createTable)
	http.HandleFunc("/insert", insertRecord)

	fmt.Println("Slave1 running on 8002")

	http.ListenAndServe(":8002", nil)
}