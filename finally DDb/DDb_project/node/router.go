package node

import (
	"net/http"
)

// routeWrite dispatches a write request to the correct master handler.
// Called only when a slave has been promoted to master.
func (m *Master) routeWrite(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/db/create":
		m.handleCreateDB(w, r)
	case "/db/drop":
		m.handleDropDB(w, r)
	case "/table/create":
		m.handleCreateTable(w, r)
	case "/table/drop":
		m.handleDropTable(w, r)
	case "/record/insert":
		m.handleInsert(w, r)
	case "/record/update":
		m.handleUpdate(w, r)
	case "/record/delete":
		m.handleDelete(w, r)
	default:
		http.NotFound(w, r)
	}
}
