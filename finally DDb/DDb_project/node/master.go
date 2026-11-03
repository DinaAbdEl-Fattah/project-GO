// Package node – Master node implementation.
// The master is the ONLY node that performs write operations on the database.
// After every write it broadcasts the full state to ALL slaves simultaneously
// using one goroutine per slave (fan-out pattern).
package node

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"distributed-db/database"
	"distributed-db/models"
)

//  Master struct
// PendingRequest represents a slave request waiting for master authorization.
type PendingRequest struct {
	ID        string `json:"id"`
	SlavePort string `json:"slave_port"`
	Action    string `json:"action"`
	Payload   string `json:"payload"`
	Response  chan bool `json:"-"`
}

// Master represents the primary (write-capable) node.
type Master struct {
	port   string
	store  *database.Store
	slaves []string    // registered slave base-URLs, e.g. ["http://localhost:8081"]
	mu     sync.RWMutex

	// Pending approvals for manual override
	pendingApprovals map[string]*PendingRequest
	pendingMu        sync.Mutex
	nextRequestID    int
}

// NewMaster creates a Master node that stores its data under data/master_<port>/.
func NewMaster(port string, initialSlaves []string) (*Master, error) {
	store, err := database.NewStore(fmt.Sprintf("data/master_%s", port))
	if err != nil {
		return nil, err
	}
	return &Master{
		port:             port,
		store:            store,
		slaves:           initialSlaves,
		pendingApprovals: make(map[string]*PendingRequest),
		nextRequestID:    1,
	}, nil
}

//  Start – registers all HTTP routes and begins listening

// Start registers every HTTP endpoint and blocks serving requests.
func (m *Master) Start() {
	mux := http.NewServeMux()

	// Infrastructure endpoints
	mux.HandleFunc("/health", m.handleHealth)
	mux.HandleFunc("/slave/register", m.handleRegisterSlave)
	mux.HandleFunc("/cluster/info", m.handleClusterInfo)
	mux.HandleFunc("/master/request-permission", m.handleRequestPermission)
	mux.HandleFunc("/master/approvals", m.handleGetApprovals)
	mux.HandleFunc("/master/approvals/decide", m.handleDecideApproval)

	// Start terminal approvals reader in background
	go m.startTerminalApprovalReader()

	//  Write endpoints (master-only) 
	mux.HandleFunc("/db/create", m.handleCreateDB)
	mux.HandleFunc("/db/drop", m.handleDropDB)
	mux.HandleFunc("/table/create", m.handleCreateTable)
	mux.HandleFunc("/table/drop", m.handleDropTable)
	mux.HandleFunc("/record/insert", m.handleInsert)
	mux.HandleFunc("/record/update", m.handleUpdate)
	mux.HandleFunc("/record/delete", m.handleDelete)

	//  Read endpoint 
	mux.HandleFunc("/record/select", m.handleSelect)
	mux.HandleFunc("/record/search", m.handleSearch)

	log.Printf("[MASTER :%s] Listening…", m.port)
	if len(m.slaves) > 0 {
		go func() {
			// Small delay to allow the server to start listening
			time.Sleep(500 * time.Millisecond)
			m.notifySlavesOfPeers()
		}()
	}
	log.Fatal(http.ListenAndServe(":"+m.port, WithCORS(mux)))
}

//  Infrastructure handlers


// handleHealth responds with the node's role so slaves can detect failure.
func (m *Master) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "alive", "role": "master", "priority": 0})
}

// handleRequestPermission evaluates whether a slave request is approved by the master.
func (m *Master) handleRequestPermission(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req models.PermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}

	m.pendingMu.Lock()
	id := fmt.Sprintf("%d", m.nextRequestID)
	m.nextRequestID++

	pending := &PendingRequest{
		ID:        id,
		SlavePort: req.SlavePort,
		Action:    req.Action,
		Payload:   req.Payload,
		Response:  make(chan bool, 1),
	}
	m.pendingApprovals[id] = pending
	m.pendingMu.Unlock()

	log.Printf("\n🔔 [PENDING REQUEST #%s] Slave on port %s wants to execute %s", id, req.SlavePort, req.Action)
	log.Printf("Payload: %s", req.Payload)
	log.Printf("Approve/Reject? Type 'y %s' or 'n %s' in this master terminal, or use Dashboard.\n", id, id)

	// Block until approved or rejected
	approved := <-pending.Response

	m.pendingMu.Lock()
	delete(m.pendingApprovals, id)
	m.pendingMu.Unlock()

	reason := "Approved by Master manual decision"
	if !approved {
		reason = "Rejected by Master manual override"
		log.Printf("🔴 [MASTER :%s] REJECTED request #%s (%s) from Slave on port %s", m.port, id, req.Action, req.SlavePort)
	} else {
		log.Printf("🟢 [MASTER :%s] APPROVED request #%s (%s) from Slave on port %s", m.port, id, req.Action, req.SlavePort)
	}

	writeJSON(w, http.StatusOK, models.PermissionResponse{
		Approved: approved,
		Reason:   reason,
	})
}

// handleRegisterSlave lets a slave add itself to the broadcast list.
func (m *Master) handleRegisterSlave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req models.RegisterSlaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	m.mu.Lock()
	alreadyRegistered := false
	for _, s := range m.slaves {
		if s == req.Address {
			alreadyRegistered = true
			break
		}
	}
	if !alreadyRegistered {
		m.slaves = append(m.slaves, req.Address)
	}
	m.mu.Unlock()

	log.Printf("[MASTER] Slave registered: %s (priority %d)", req.Address, req.Priority)
	go m.notifySlavesOfPeers()

	if alreadyRegistered {
		writeSuccess(w, "already registered", nil)
	} else {
		writeSuccess(w, "registered", nil)
	}
}


//  Write handlers  (each calls broadcastReplication after success)

func (m *Master) handleCreateDB(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req models.CreateDBRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	if err := m.store.CreateDB(req.DBName); err != nil {
		writeError(w, err.Error())
		return
	}
	go m.broadcastReplication() // replicate asynchronously after responding
	writeSuccess(w, "database created: "+req.DBName, nil)
}

func (m *Master) handleDropDB(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "DELETE only", http.StatusMethodNotAllowed)
		return
	}
	var req models.DropDBRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	if err := m.store.DropDB(req.DBName); err != nil {
		writeError(w, err.Error())
		return
	}
	go m.broadcastReplication()
	writeSuccess(w, "database dropped: "+req.DBName, nil)
}

func (m *Master) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req models.CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	// Convert models.ColumnDef → database.Column
	cols := make([]database.Column, len(req.Attributes))
	for i, a := range req.Attributes {
		cols[i] = database.Column{Name: a.Name, Type: a.Type}
	}
	if err := m.store.CreateTable(req.DBName, req.TableName, cols); err != nil {
		writeError(w, err.Error())
		return
	}
	go m.broadcastReplication()
	writeSuccess(w, "table created: "+req.TableName, nil)
}

func (m *Master) handleDropTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "DELETE only", http.StatusMethodNotAllowed)
		return
	}
	var req models.DropTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	if err := m.store.DropTable(req.DBName, req.TableName); err != nil {
		writeError(w, err.Error())
		return
	}
	go m.broadcastReplication()
	writeSuccess(w, "table dropped: "+req.TableName, nil)
}

func (m *Master) handleInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req models.InsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	if err := m.store.Insert(req.DBName, req.TableName, req.Record); err != nil {
		writeError(w, err.Error())
		return
	}
	go m.broadcastReplication()
	writeSuccess(w, "record inserted", nil)
}

func (m *Master) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "PUT only", http.StatusMethodNotAllowed)
		return
	}
	var req models.UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	count, err := m.store.Update(req.DBName, req.TableName, req.Condition, req.Updates)
	if err != nil {
		writeError(w, err.Error())
		return
	}
	go m.broadcastReplication()
	writeSuccess(w, fmt.Sprintf("%d record(s) updated", count), nil)
}

func (m *Master) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "DELETE only", http.StatusMethodNotAllowed)
		return
	}
	var req models.DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	count, err := m.store.Delete(req.DBName, req.TableName, req.Condition)
	if err != nil {
		writeError(w, err.Error())
		return
	}
	go m.broadcastReplication()
	writeSuccess(w, fmt.Sprintf("%d record(s) deleted", count), nil)
}


//  Read handler


func (m *Master) handleSelect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req models.SelectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	rows, err := m.store.Select(req.DBName, req.TableName, req.Condition)
	if err != nil {
		writeError(w, err.Error())
		return
	}
	writeSuccess(w, "ok", rows)
}

func (m *Master) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req models.SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}
	rows, err := m.store.Search(req.DBName, req.TableName, req.Query)
	if err != nil {
		writeError(w, err.Error())
		return
	}
	writeSuccess(w, "ok", rows)
}

func (m *Master) handleClusterInfo(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info := map[string]interface{}{
		"role":   "master",
		"slaves": m.slaves,
	}
	writeJSON(w, http.StatusOK, info)
}

//  Broadcast – fan-out replication to all slaves simultaneously

// broadcastReplication sends the full database snapshot to every registered
// slave at the same time using one goroutine per slave (fan-out).
// It waits for all goroutines to finish before returning.
func (m *Master) broadcastReplication() {
	snapshot, err := m.store.GetSnapshotJSON()
	if err != nil {
		log.Printf("[MASTER] snapshot error: %v", err)
		return
	}

	// Take a safe copy of the slave list
	m.mu.RLock()
	slaves := make([]string, len(m.slaves))
	copy(slaves, m.slaves)
	m.mu.RUnlock()

	if len(slaves) == 0 {
		return
	}

	var wg sync.WaitGroup
	// ─── KEY REQUIREMENT: one goroutine per slave → all receive simultaneously ───
	for _, addr := range slaves {
		wg.Add(1)
		go func(slaveAddr string) { // each slave gets its own goroutine
			defer wg.Done()
			resp, err := http.Post(slaveAddr+"/replicate", "application/json",
				bytes.NewReader(snapshot))
			if err != nil {
				log.Printf("[MASTER] replication to %s failed: %v", slaveAddr, err)
				return
			}
			resp.Body.Close()
			log.Printf("[MASTER] replicated to %s ✓", slaveAddr)
		}(addr)
	}
	wg.Wait() // wait for ALL goroutines to finish
}

// notifySlavesOfPeers broadcasts the updated list of sibling peers to each registered slave.
func (m *Master) notifySlavesOfPeers() {
	m.mu.RLock()
	slaves := make([]string, len(m.slaves))
	copy(slaves, m.slaves)
	m.mu.RUnlock()

	for _, target := range slaves {
		// peers list for target is all other slaves in the cluster except target
		var peers []string
		for _, s := range slaves {
			if s != target {
				peers = append(peers, s)
			}
		}

		go func(targetSlave string, slavePeers []string) {
			payload, err := json.Marshal(map[string][]string{"peers": slavePeers})
			if err != nil {
				log.Printf("[MASTER] failed to marshal peers payload: %v", err)
				return
			}
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Post(targetSlave+"/update-peers", "application/json", bytes.NewReader(payload))
			if err != nil {
				log.Printf("[MASTER] failed to update peers for slave %s: %v", targetSlave, err)
				return
			}
			resp.Body.Close()
			log.Printf("[MASTER] updated peers list for %s -> %v", targetSlave, slavePeers)
		}(target, peers)
	}
}

//  Helpers used by promoted slave

// GetStore exposes the store so a promoted slave can share state.
func (m *Master) GetStore() *database.Store { return m.store }

// AddSlave adds a peer address to the broadcast list (used after promotion).
func (m *Master) AddSlave(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.slaves {
		if s == addr {
			return
		}
	}
	m.slaves = append(m.slaves, addr)
}

// BroadcastReplication is the exported version for use by promoted slave.
func (m *Master) BroadcastReplication() { m.broadcastReplication() }

// ServeHTTP lets the promoted slave re-use master's mux by embedding it.
func (m *Master) ServeReplicateEndpoint(mux *http.ServeMux) {
	mux.HandleFunc("/replicate", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := m.store.ApplySnapshotJSON(body); err != nil {
			writeError(w, err.Error())
			return
		}
		writeSuccess(w, "replicated", nil)
	})
}

// handleGetApprovals lists all currently pending slave request approvals.
func (m *Master) handleGetApprovals(w http.ResponseWriter, r *http.Request) {
	m.pendingMu.Lock()
	list := make([]*PendingRequest, 0, len(m.pendingApprovals))
	for _, req := range m.pendingApprovals {
		list = append(list, req)
	}
	m.pendingMu.Unlock()
	writeJSON(w, http.StatusOK, list)
}

// handleDecideApproval unblocks and decides on a request via the dashboard API.
func (m *Master) handleDecideApproval(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID       string `json:"id"`
		Decision string `json:"decision"` // "approve" or "reject"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}

	m.pendingMu.Lock()
	pending, exists := m.pendingApprovals[req.ID]
	m.pendingMu.Unlock()

	if !exists {
		writeJSON(w, http.StatusNotFound, models.Response{Success: false, Message: "Request not found or already decided"})
		return
	}

	approved := req.Decision == "approve"
	pending.Response <- approved

	log.Printf("[MASTER :%s] DECIDED [REQUEST #%s] -> %s (via Dashboard)", m.port, req.ID, req.Decision)
	writeJSON(w, http.StatusOK, models.Response{Success: true, Message: "Decision applied"})
}

// startTerminalApprovalReader polls standard input for y <id> / n <id> inputs.
func (m *Master) startTerminalApprovalReader() {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		parts := strings.Fields(line)
		if len(parts) != 2 {
			continue
		}
		cmd := strings.ToLower(parts[0])
		id := parts[1]

		m.pendingMu.Lock()
		pending, exists := m.pendingApprovals[id]
		m.pendingMu.Unlock()

		if !exists {
			fmt.Printf("⚠️ Request ID %s not found or already decided.\n", id)
			continue
		}

		if cmd == "y" || cmd == "yes" || cmd == "approve" {
			pending.Response <- true
			fmt.Printf("✅ Approved request #%s\n", id)
		} else if cmd == "n" || cmd == "no" || cmd == "reject" {
			pending.Response <- false
			fmt.Printf("❌ Rejected request #%s\n", id)
		} else {
			fmt.Printf("⚠️ Unknown command '%s'. Use 'y <id>' or 'n <id>'\n", cmd)
		}
	}
}
