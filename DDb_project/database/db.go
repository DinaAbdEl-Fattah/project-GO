// Package database provides an in-memory database engine backed by JSON files on disk.
package database

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)


//  Data structures

// Column represents a single column definition inside a table.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Table holds a table's schema and all its rows.
type Table struct {
	Name    string                   `json:"name"`
	Columns []Column                 `json:"columns"`
	Records []map[string]interface{} `json:"records"`
}

// Database is a named collection of tables.
type Database struct {
	Name   string            `json:"name"`
	Tables map[string]*Table `json:"tables"`
}

// ─────────────────────────────────────────────────────────────
//  Store – the central storage engine
// ─────────────────────────────────────────────────────────────

// Store manages all databases in memory and persists them to disk.
type Store struct {
	mu        sync.RWMutex
	databases map[string]*Database
	dataDir   string // directory where each DB is saved as <name>.json
}

// NewStore creates (or loads) a store rooted at dataDir.
func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("cannot create data directory: %w", err)
	}
	s := &Store{
		databases: make(map[string]*Database),
		dataDir:   dataDir,
	}
	s.loadFromDisk() // restore any previously persisted databases
	return s, nil
}

// ─────────────────────────────────────────────────────────────
//  Disk helpers
// ─────────────────────────────────────────────────────────────

func (s *Store) filePath(dbName string) string {
	return fmt.Sprintf("%s/%s.json", s.dataDir, dbName)
}

// loadFromDisk reads all *.json files from dataDir into memory.
func (s *Store) loadFromDisk() {
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(s.dataDir + "/" + e.Name())
		if err != nil {
			continue
		}
		var db Database
		if err := json.Unmarshal(data, &db); err != nil {
			continue
		}
		if db.Tables == nil {
			db.Tables = make(map[string]*Table)
		}
		s.databases[db.Name] = &db
	}
}

// saveToDisk persists a single database to its JSON file (caller must hold write lock).
func (s *Store) saveToDisk(db *Database) error {
	data, err := json.MarshalIndent(db, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.filePath(db.Name), data, 0644)
}

// ─────────────────────────────────────────────────────────────
//  Database-level operations
// ─────────────────────────────────────────────────────────────

// CreateDB creates a new empty database.
func (s *Store) CreateDB(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.databases[name]; ok {
		return fmt.Errorf("database '%s' already exists", name)
	}
	db := &Database{Name: name, Tables: make(map[string]*Table)}
	s.databases[name] = db
	return s.saveToDisk(db)
}

// DropDB removes a database entirely.
func (s *Store) DropDB(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.databases[name]; !ok {
		return fmt.Errorf("database '%s' not found", name)
	}
	delete(s.databases, name)
	_ = os.Remove(s.filePath(name))
	return nil
}

// ─────────────────────────────────────────────────────────────
//  Table-level operations
// ─────────────────────────────────────────────────────────────

// CreateTable adds a new table with the given columns to an existing database.
func (s *Store) CreateTable(dbName, tableName string, columns []Column) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, ok := s.databases[dbName]
	if !ok {
		return fmt.Errorf("database '%s' not found", dbName)
	}
	if _, ok := db.Tables[tableName]; ok {
		return fmt.Errorf("table '%s' already exists", tableName)
	}
	db.Tables[tableName] = &Table{
		Name:    tableName,
		Columns: columns,
		Records: []map[string]interface{}{},
	}
	return s.saveToDisk(db)
}

// DropTable removes a table from a database.
func (s *Store) DropTable(dbName, tableName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, ok := s.databases[dbName]
	if !ok {
		return fmt.Errorf("database '%s' not found", dbName)
	}
	if _, ok := db.Tables[tableName]; !ok {
		return fmt.Errorf("table '%s' not found", tableName)
	}
	delete(db.Tables, tableName)
	return s.saveToDisk(db)
}

// ─────────────────────────────────────────────────────────────
//  Record-level operations
// ─────────────────────────────────────────────────────────────

// Insert appends a new record to the specified table.
func (s *Store) Insert(dbName, tableName string, record map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, ok := s.databases[dbName]
	if !ok {
		return fmt.Errorf("database '%s' not found", dbName)
	}
	tbl, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table '%s' not found", tableName)
	}
	tbl.Records = append(tbl.Records, record)
	return s.saveToDisk(db)
}

// Select returns records that match all condition key-value pairs.
// An empty condition returns all records.
func (s *Store) Select(dbName, tableName string, condition map[string]interface{}) ([]map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db, ok := s.databases[dbName]
	if !ok {
		return nil, fmt.Errorf("database '%s' not found", dbName)
	}
	tbl, ok := db.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table '%s' not found", tableName)
	}
	if len(condition) == 0 {
		return tbl.Records, nil
	}
	var result []map[string]interface{}
	for _, rec := range tbl.Records {
		if matches(rec, condition) {
			result = append(result, rec)
		}
	}
	return result, nil
}

// Search returns records that contain the query string in any of their field values (case-insensitive).
func (s *Store) Search(dbName, tableName, query string) ([]map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db, ok := s.databases[dbName]
	if !ok {
		return nil, fmt.Errorf("database '%s' not found", dbName)
	}
	tbl, ok := db.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table '%s' not found", tableName)
	}
	if query == "" {
		return tbl.Records, nil
	}
	lowerQuery := strings.ToLower(query)
	var result []map[string]interface{}
	for _, rec := range tbl.Records {
		match := false
		for _, val := range rec {
			valStr := fmt.Sprintf("%v", val)
			if strings.Contains(strings.ToLower(valStr), lowerQuery) {
				match = true
				break
			}
		}
		if match {
			result = append(result, rec)
		}
	}
	return result, nil
}


// Update modifies fields in all records that satisfy condition.
// Returns the number of affected rows.
func (s *Store) Update(dbName, tableName string, condition, updates map[string]interface{}) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, ok := s.databases[dbName]
	if !ok {
		return 0, fmt.Errorf("database '%s' not found", dbName)
	}
	tbl, ok := db.Tables[tableName]
	if !ok {
		return 0, fmt.Errorf("table '%s' not found", tableName)
	}
	count := 0
	for _, rec := range tbl.Records {
		// Maps are reference types – modifying rec modifies the slice element directly.
		if len(condition) == 0 || matches(rec, condition) {
			for k, v := range updates {
				rec[k] = v
			}
			count++
		}
	}
	if count > 0 {
		return count, s.saveToDisk(db)
	}
	return 0, nil
}

// Delete removes all records that satisfy condition.
// Returns the number of deleted rows.
func (s *Store) Delete(dbName, tableName string, condition map[string]interface{}) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, ok := s.databases[dbName]
	if !ok {
		return 0, fmt.Errorf("database '%s' not found", dbName)
	}
	tbl, ok := db.Tables[tableName]
	if !ok {
		return 0, fmt.Errorf("table '%s' not found", tableName)
	}
	original := len(tbl.Records)
	kept := tbl.Records[:0]
	for _, rec := range tbl.Records {
		if len(condition) > 0 && !matches(rec, condition) {
			kept = append(kept, rec)
		}
	}
	tbl.Records = kept
	deleted := original - len(kept)
	if deleted > 0 {
		return deleted, s.saveToDisk(db)
	}
	return 0, nil
}

// ─────────────────────────────────────────────────────────────
//  Replication helpers
// ─────────────────────────────────────────────────────────────

// GetSnapshotJSON serialises the entire state to JSON (used for replication).
func (s *Store) GetSnapshotJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.databases)
}

// ApplySnapshotJSON replaces the entire state from a JSON snapshot received from master.
func (s *Store) ApplySnapshotJSON(data []byte) error {
	var dbs map[string]*Database
	if err := json.Unmarshal(data, &dbs); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Fix nil Tables maps that may occur after unmarshal
	for _, db := range dbs {
		if db.Tables == nil {
			db.Tables = make(map[string]*Table)
		}
	}
	s.databases = dbs
	// Persist each database to disk
	for _, db := range dbs {
		_ = s.saveToDisk(db)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────
//  Private helpers
// ─────────────────────────────────────────────────────────────

// matches returns true when every key-value pair in condition is present in record.
func matches(record, condition map[string]interface{}) bool {
	for k, v := range condition {
		// JSON numbers are decoded as float64; compare string representations for safety.
		if fmt.Sprintf("%v", record[k]) != fmt.Sprintf("%v", v) {
			return false
		}
	}
	return true
}
