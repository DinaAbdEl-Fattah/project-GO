package shared

type Record map[string]string

type Table struct {
	Name    string
	Records []Record
}

type Database struct {
	Name   string
	Tables map[string]*Table
}