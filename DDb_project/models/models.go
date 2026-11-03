package models


//  Request structs – sent by clients via HTTP JSON

// CreateDBRequest is the payload for creating a new database.
type CreateDBRequest struct {
	DBName string `json:"db_name"`
}

// DropDBRequest is the payload for dropping an existing database.
type DropDBRequest struct {
	DBName string `json:"db_name"`
}

// ColumnDef describes a single column (name + type).
type ColumnDef struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// CreateTableRequest is the payload for creating a new table.
type CreateTableRequest struct {
	DBName     string      `json:"db_name"`
	TableName  string      `json:"table_name"`
	Attributes []ColumnDef `json:"attributes"` // list of column definitions
}

// DropTableRequest is the payload for dropping a table.
type DropTableRequest struct {
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
}

// InsertRequest is the payload for inserting a record.
type InsertRequest struct {
	DBName    string                 `json:"db_name"`
	TableName string                 `json:"table_name"`
	Record    map[string]interface{} `json:"record"`
}

// UpdateRequest is the payload for updating records that match Condition.
type UpdateRequest struct {
	DBName    string                 `json:"db_name"`
	TableName string                 `json:"table_name"`
	Condition map[string]interface{} `json:"condition"` // field→value filter (empty = update all)
	Updates   map[string]interface{} `json:"updates"`   // fields to change
}

// DeleteRequest is the payload for deleting records that match Condition.
type DeleteRequest struct {
	DBName    string                 `json:"db_name"`
	TableName string                 `json:"table_name"`
	Condition map[string]interface{} `json:"condition"` // field→value filter (empty = delete all)
}

// SelectRequest is the payload for querying records.
type SelectRequest struct {
	DBName    string                 `json:"db_name"`
	TableName string                 `json:"table_name"`
	Condition map[string]interface{} `json:"condition"` // empty = return all rows
}

// SearchRequest is the payload for searching records by substring.
type SearchRequest struct {
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
	Query     string `json:"query"` // substring to search in any field value
}

//  Response struct – returned by all endpoints

// Response is the standard envelope returned by every endpoint.
type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

//  Internal communication structs

// RegisterSlaveRequest is sent by a slave to register itself with the master.
type RegisterSlaveRequest struct {
	Address  string `json:"address"`  // e.g. "http://localhost:8081"
	Priority int    `json:"priority"` // lower = higher chance to win election
}

// NewMasterNotification is broadcast by the promoted slave to its peers.
type NewMasterNotification struct {
	MasterAddress string `json:"master_address"`
}

// PermissionRequest is sent by a slave to ask the master for write permission.
type PermissionRequest struct {
	SlavePort string `json:"slave_port"`
	Action    string `json:"action"`  // e.g. "/record/insert"
	Payload   string `json:"payload"` // JSON-encoded body of the write request
}

// PermissionResponse is returned by the master.
type PermissionResponse struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason"`
}
