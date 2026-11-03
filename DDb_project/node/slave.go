// Package node – Slave node implementation.
// Slaves are READ-ONLY by default.
// Any write request is proxied (forwarded) to the master.
// The master executes the write, then broadcasts the new state back to ALL slaves.
// If the master dies, the slave with the lowest priority promotes itself to master.
package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"distributed-db/database"
	"distributed-db/models"
)

//  Slave struct

// Slave represents a read-only replica node.
type Slave struct {
	port       string
	masterAddr string   // base-URL of master, e.g. "http://localhost:8080"
	peers      []string // base-URLs of sibling slaves
	priority   int      // lower number = promoted first during election
	slaveIP    string   // IP address or hostname of this slave
	store      *database.Store
	mu         sync.RWMutex

	// Promotion state
	isMaster bool    // true once this slave wins leader election
	promoted *Master // non-nil after promotion
}

// NewSlave creates a Slave node.
// peers is a list of the other slaves' addresses (excluding this slave itself).
// priority 1 = highest priority (promoted first).
// slaveIP is the IP address or hostname of this slave node (used for registration).
func NewSlave(port, masterAddr string, peers []string, priority int, slaveIP string) (*Slave, error) {
	store, err := database.NewStore(fmt.Sprintf("data/slave_%s", port))
	if err != nil {
		return nil, err
	}
	return &Slave{
		port:       port,
		masterAddr: masterAddr,
		peers:      peers,
		priority:   priority,
		store:      store,
		slaveIP:    slaveIP,
	}, nil
}

//  Start – register routes, connect to master, start heartbeat

// Start registers HTTP endpoints, registers with master, and starts the heartbeat goroutine.
func (s *Slave) Start() {
	mux := http.NewServeMux()

	// ── Infrastructure ──────────────────────────────────────────
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/replicate", s.handleReplicate)  // master pushes state here
	mux.HandleFunc("/new-master", s.handleNewMaster) // notified of new master after election
	mux.HandleFunc("/cluster/info", s.handleClusterInfo)
	mux.HandleFunc("/update-peers", s.handleUpdatePeers) // master pushes sibling peer addresses here

	// ── Read endpoint (served from LOCAL copy) ──────────────────
	mux.HandleFunc("/record/select", s.handleSelect)
	mux.HandleFunc("/record/search", s.handleSearch)

	// ── Write endpoints (proxied to master, or executed if promoted) ─
	mux.HandleFunc("/db/create", s.handleWrite)
	mux.HandleFunc("/db/drop", s.handleWrite)
	mux.HandleFunc("/table/create", s.handleWrite)
	mux.HandleFunc("/table/drop", s.handleWrite)
	mux.HandleFunc("/record/insert", s.handleWrite)
	mux.HandleFunc("/record/update", s.handleWrite)
	mux.HandleFunc("/record/delete", s.handleWrite)

	// ── Master delegation endpoints (active only when promoted) ──
	mux.HandleFunc("/slave/register", s.handleMasterDelegateRegister)
	mux.HandleFunc("/master/request-permission", s.handleMasterDelegateRequestPermission)
	mux.HandleFunc("/master/approvals", s.handleMasterDelegateApprovals)
	mux.HandleFunc("/master/approvals/decide", s.handleMasterDelegateDecideApproval)

	// Register with master (retry a few times in case master is not up yet)
	go s.registerWithMaster()

	// Heartbeat goroutine – detects master failure and triggers election
	go s.heartbeat()

	log.Printf("[SLAVE :%s] Listening… (priority %d, master %s)", s.port, s.priority, s.masterAddr)
	log.Fatal(http.ListenAndServe(":"+s.port, WithCORS(mux)))
}

//  Registration

// registerWithMaster sends this slave's address to master so it appears on the broadcast list.
func (s *Slave) registerWithMaster() {
	myAddr := "http://" + s.slaveIP + ":" + s.port
	payload, _ := json.Marshal(models.RegisterSlaveRequest{
		Address:  myAddr,
		Priority: s.priority,
	})

	// Retry up to 10 times with a 2-second gap (master may start slightly later)
	for i := 0; i < 10; i++ {
		s.mu.RLock()
		master := s.masterAddr
		s.mu.RUnlock()

		resp, err := http.Post(master+"/slave/register", "application/json",
			bytes.NewReader(payload))
		if err == nil {
			resp.Body.Close()
			log.Printf("[SLAVE :%s] Registered with master %s", s.port, master)
			return
		}
		log.Printf("[SLAVE :%s] Registration attempt %d failed: %v", s.port, i+1, err)
		time.Sleep(2 * time.Second)
	}
	log.Printf("[SLAVE :%s] Could not register with master after 10 attempts", s.port)
}

//  Heartbeat & Leader Election

// heartbeat pings the master every 3 seconds.
// Three consecutive failures trigger leader election.
func (s *Slave) heartbeat() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	failures := 0

	for range ticker.C {
		// Skip heartbeat if this slave is already the new master
		s.mu.RLock()
		isM := s.isMaster
		master := s.masterAddr
		s.mu.RUnlock()
		if isM {
			return
		}

		resp, err := http.Get(master + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			failures = 0
			resp.Body.Close()
		} else {
			failures++
			log.Printf("[SLAVE :%s] Master heartbeat failed (%d/3)", s.port, failures)
			if failures >= 3 {
				s.triggerElection()
				failures = 0
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
	}
}

// triggerElection decides whether this slave should become the new master.
// Strategy: query all peers. Only promote if all online peers have lower priority (higher priority number) than us.
func (s *Slave) triggerElection() {
	log.Printf("[SLAVE :%s] Starting election (priority %d)…", s.port, s.priority)

	s.mu.RLock()
	peers := make([]string, len(s.peers))
	copy(peers, s.peers)
	s.mu.RUnlock()

	client := &http.Client{Timeout: 2 * time.Second}
	shouldPromote := true

	for _, peer := range peers {
		resp, err := client.Get(peer + "/health")
		if err != nil {
			log.Printf("[SLAVE :%s] Peer %s is offline/unreachable", s.port, peer)
			continue
		}
		
		var health struct {
			Status   string `json:"status"`
			Role     string `json:"role"`
			Priority int    `json:"priority"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
			resp.Body.Close()
			log.Printf("[SLAVE :%s] Failed to decode health from peer %s: %v", s.port, peer, err)
			continue
		}
		resp.Body.Close()

		// If another peer is already master, update our master address and stop election
		if health.Role == "master" {
			s.mu.Lock()
			s.masterAddr = peer
			s.mu.Unlock()
			log.Printf("[SLAVE :%s] Existing master detected: %s. Stopping election.", s.port, peer)
			go s.registerWithMaster() // re-register with the new master
			return
		}

		// If a peer is online and has higher priority (lower priority number) than us, do NOT promote
		if health.Priority < s.priority {
			log.Printf("[SLAVE :%s] Online peer %s has higher priority (%d < %d). We will NOT promote.", s.port, peer, health.Priority, s.priority)
			shouldPromote = false
		}
	}

	if shouldPromote {
		s.promoteToMaster()
	} else {
		log.Printf("[SLAVE :%s] Standing by. Waiting for new master promotion...", s.port)
	}
}

// promoteToMaster turns this slave into the new master.
func (s *Slave) promoteToMaster() {
	log.Printf("[SLAVE :%s] 👑 Promoting to MASTER", s.port)

	s.mu.RLock()
	peers := make([]string, len(s.peers))
	copy(peers, s.peers)
	s.mu.RUnlock()

	// Create a Master instance reusing this slave's data store
	promoted, err := NewMaster(s.port, peers)
	if err != nil {
		log.Printf("[SLAVE :%s] Promotion failed: %v", s.port, err)
		return
	}
	// Share the same on-disk data directory by applying current snapshot
	snap, _ := s.store.GetSnapshotJSON()
	_ = promoted.GetStore().ApplySnapshotJSON(snap)

	myAddr := "http://" + s.slaveIP + ":" + s.port

	s.mu.Lock()
	s.isMaster = true
	s.promoted = promoted
	s.masterAddr = myAddr
	s.mu.Unlock()

	// Start terminal approvals reader in background
	go promoted.startTerminalApprovalReader()

	// Notify all peers about the leadership change
	notification, _ := json.Marshal(models.NewMasterNotification{MasterAddress: myAddr})
	for _, peer := range peers {
		go func(p string) {
			resp, err := http.Post(p+"/new-master", "application/json",
				bytes.NewReader(notification))
			if err == nil {
				resp.Body.Close()
			}
		}(peer)
	}

	// Broadcast the current state so all peers are up to date
	go promoted.BroadcastReplication()
}

//  HTTP Handlers

// handleHealth reports this node's role (slave or master after promotion) and priority.
func (s *Slave) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	role := "slave"
	if s.isMaster {
		role = "master"
	}
	pri := s.priority
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "alive", "role": role, "priority": pri})
}

// handleReplicate receives a full state snapshot from the master and applies it locally.
func (s *Slave) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, "failed to read body")
		return
	}

	// If this slave was promoted to master, replicate to the master's store
	s.mu.RLock()
	promoted := s.promoted
	store := s.store
	s.mu.RUnlock()

	if promoted != nil {
		store = promoted.GetStore()
	}

	if err := store.ApplySnapshotJSON(body); err != nil {
		writeError(w, err.Error())
		return
	}
	log.Printf("[SLAVE :%s] State replicated ✓", s.port)
	writeSuccess(w, "replicated", nil)
}

// handleNewMaster updates this slave's master address after a leadership change.
func (s *Slave) handleNewMaster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var note models.NewMasterNotification
	if err := json.NewDecoder(r.Body).Decode(&note); err != nil {
		writeError(w, err.Error())
		return
	}
	s.mu.Lock()
	s.masterAddr = note.MasterAddress
	s.mu.Unlock()
	log.Printf("[SLAVE :%s] Master updated to %s", s.port, note.MasterAddress)
	go s.registerWithMaster() // register with the new master
	writeSuccess(w, "master updated", nil)
}

// handleSelect reads directly from the local replica (no forwarding needed).
func (s *Slave) handleSelect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	// If this slave was promoted, delegate to the master's store
	s.mu.RLock()
	promoted := s.promoted
	s.mu.RUnlock()
	if promoted != nil {
		promoted.handleSelect(w, r) // use master handler directly
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, "failed to read request body")
		return
	}

	approved, reason := s.askPermission(r.URL.Path, body)
	if !approved {
		log.Printf("🔴 [SLAVE :%s] Read Select permission DENIED by Master. Reason: %s", s.port, reason)
		writeJSON(w, http.StatusForbidden, models.Response{
			Success: false,
			Message: "Permission denied by Master: " + reason,
		})
		return
	}

	var req models.SelectRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, err.Error())
		return
	}
	rows, err := s.store.Select(req.DBName, req.TableName, req.Condition)
	if err != nil {
		writeError(w, err.Error())
		return
	}
	writeSuccess(w, "ok", rows)
}

func (s *Slave) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	promoted := s.promoted
	s.mu.RUnlock()
	if promoted != nil {
		promoted.handleSearch(w, r)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, "failed to read request body")
		return
	}

	approved, reason := s.askPermission(r.URL.Path, body)
	if !approved {
		log.Printf("🔴 [SLAVE :%s] Read Search permission DENIED by Master. Reason: %s", s.port, reason)
		writeJSON(w, http.StatusForbidden, models.Response{
			Success: false,
			Message: "Permission denied by Master: " + reason,
		})
		return
	}

	var req models.SearchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, err.Error())
		return
	}
	rows, err := s.store.Search(req.DBName, req.TableName, req.Query)
	if err != nil {
		writeError(w, err.Error())
		return
	}
	writeSuccess(w, "ok", rows)
}

func (s *Slave) handleClusterInfo(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	role := "slave"
	var slaves []string
	if s.isMaster {
		role = "master"
		if s.promoted != nil {
			slaves = s.promoted.slaves
		}
	} else {
		slaves = make([]string, len(s.peers))
		copy(slaves, s.peers)
	}

	info := map[string]interface{}{
		"role":   role,
		"master": s.masterAddr,
		"slaves": slaves,
	}
	writeJSON(w, http.StatusOK, info)
}

// handleWrite either proxies the request to master (normal slave mode)
// or executes it locally (after promotion to master).
func (s *Slave) handleWrite(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	promoted := s.promoted
	s.mu.RUnlock()

	if promoted != nil {
		// This slave is now the master – delegate to master's mux handlers
		promoted.routeWrite(w, r)
		return
	}

	// Normal slave: proxy the request transparently to the real master
	s.proxyToMaster(w, r)
}

// proxyToMaster forwards the incoming request to the master unchanged
// and pipes the master's response back to the original client.
func (s *Slave) proxyToMaster(w http.ResponseWriter, r *http.Request) {
	// Buffer the body so we can forward it
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, "failed to read request body")
		return
	}

	s.mu.RLock()
	masterAddr := s.masterAddr
	s.mu.RUnlock()

	approved, reason := s.askPermission(r.URL.Path, body)
	if !approved {
		log.Printf("🔴 [SLAVE :%s] Permission DENIED by Master. Reason: %s", s.port, reason)
		writeJSON(w, http.StatusForbidden, models.Response{
			Success: false,
			Message: "Permission denied by Master: " + reason,
		})
		return
	}

	log.Printf("🟢 [SLAVE :%s] Permission APPROVED by Master. Proxying request...", s.port)

	// Proceed with proxying
	targetURL := masterAddr + r.URL.Path // same path on master

	req, err := http.NewRequest(r.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		writeError(w, "failed to build forward request")
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		writeError(w, "master unavailable: "+err.Error())
		return
	}
	defer resp.Body.Close()

	// Pipe master's response back to the original client
	respBody, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)

	log.Printf("[SLAVE :%s] Forwarded %s %s → master", s.port, r.Method, r.URL.Path)
}

// askPermission sends a permission request to the Master and blocks until approved/rejected.
func (s *Slave) askPermission(action string, body []byte) (bool, string) {
	s.mu.RLock()
	masterAddr := s.masterAddr
	s.mu.RUnlock()

	permReq := models.PermissionRequest{
		SlavePort: s.port,
		Action:    action,
		Payload:   string(body),
	}
	permPayload, _ := json.Marshal(permReq)

	log.Printf("[SLAVE :%s] Asking MASTER (%s) for permission for %s...", s.port, masterAddr, action)
	
	// Use a longer timeout (e.g. 60 seconds) because the master decision is manual (terminal or dashboard)
	client := &http.Client{Timeout: 60 * time.Second}
	permResp, err := client.Post(masterAddr+"/master/request-permission", "application/json", bytes.NewReader(permPayload))
	if err != nil {
		return false, "Master is offline: " + err.Error()
	}
	defer permResp.Body.Close()

	var perm models.PermissionResponse
	if err := json.NewDecoder(permResp.Body).Decode(&perm); err != nil {
		return false, "Failed to decode Master response: " + err.Error()
	}
	return perm.Approved, perm.Reason
}

// handleUpdatePeers updates this slave's list of sibling peers.
func (s *Slave) handleUpdatePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Peers []string `json:"peers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error())
		return
	}

	s.mu.Lock()
	s.peers = req.Peers
	s.mu.Unlock()

	log.Printf("[SLAVE :%s] Dynamically updated peers list to: %v", s.port, req.Peers)
	writeSuccess(w, "peers updated", nil)
}

func (s *Slave) handleMasterDelegateRegister(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	promoted := s.promoted
	s.mu.RUnlock()
	if promoted != nil {
		promoted.handleRegisterSlave(w, r)
	} else {
		http.Error(w, "Not a master node", http.StatusForbidden)
	}
}

func (s *Slave) handleMasterDelegateRequestPermission(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	promoted := s.promoted
	s.mu.RUnlock()
	if promoted != nil {
		promoted.handleRequestPermission(w, r)
	} else {
		http.Error(w, "Not a master node", http.StatusForbidden)
	}
}

func (s *Slave) handleMasterDelegateApprovals(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	promoted := s.promoted
	s.mu.RUnlock()
	if promoted != nil {
		promoted.handleGetApprovals(w, r)
	} else {
		http.Error(w, "Not a master node", http.StatusForbidden)
	}
}

func (s *Slave) handleMasterDelegateDecideApproval(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	promoted := s.promoted
	s.mu.RUnlock()
	if promoted != nil {
		promoted.handleDecideApproval(w, r)
	} else {
		http.Error(w, "Not a master node", http.StatusForbidden)
	}
}
