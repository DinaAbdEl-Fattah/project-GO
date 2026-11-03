let currentConnection = "http://localhost:8080";
let discoveredNodes = ["http://localhost:8080"];
let isConnected = false;
let connectedRole = "unknown"; // tracks whether the connected node is 'master' or 'slave'

const consoleOutput = document.getElementById('console-output');
const nodesContainer = document.getElementById('nodes-container');
const targetNodeSelect = document.getElementById('target-node');
const statusBadge = document.getElementById('cluster-status-badge');

function normalizeUrl(url) {
    if (!url) return "";
    url = url.trim();
    if (!url.startsWith("http://") && !url.startsWith("https://")) {
        url = "http://" + url;
    }
    if (url.endsWith("/")) {
        url = url.slice(0, -1);
    }
    return url;
}

function logToConsole(message, isError = false) {
    const time = new Date().toLocaleTimeString();
    const entry = document.createElement('div');
    entry.className = 'console-entry';
    entry.style.color = isError ? '#f87171' : '#34d399';
    
    let text = message;
    if (typeof message === 'object') {
        text = JSON.stringify(message, null, 2);
    }
    
    entry.innerHTML = `<span class="console-time">[${time}]</span> ${text}`;
    consoleOutput.prepend(entry);
}

function clearConsole() {
    consoleOutput.innerHTML = 'Logs cleared. Ready...';
}

// Tab Switching
function switchTab(tabId) {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        if (btn.getAttribute('onclick').includes(tabId)) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.remove('active');
    });
    document.getElementById(`tab-${tabId}`).classList.add('active');
}

// Connect to Cluster Entry Node
async function connectToCluster() {
    const inputUrl = normalizeUrl(document.getElementById('connection-url').value);
    logToConsole(`Connecting to node ${inputUrl}...`);
    
    try {
        const res = await fetch(`${inputUrl}/cluster/info`, { method: 'GET' });
        const info = await res.json();
        
        currentConnection = inputUrl;
        isConnected = true;
        connectedRole = info.role;
        
        statusBadge.textContent = `Connected (${info.role.toUpperCase()})`;
        statusBadge.className = "cluster-badge connected";
        
        logToConsole(`Successfully connected! Node role: ${info.role.toUpperCase()}`);
        
        // Show or hide the Master Authorization Queue
        updateApprovalsVisibility();
        
        // Discover all unique node URLs in the cluster
        const newSet = new Set();
        newSet.add(normalizeUrl(currentConnection));
        
        if (info.master) {
            newSet.add(normalizeUrl(info.master));
        }
        if (info.slaves) {
            info.slaves.forEach(slave => newSet.add(normalizeUrl(slave)));
        }
        
        discoveredNodes = Array.from(newSet);
        
        // Build Select Target Node dropdown options
        updateDropdownOptions();
        
        // Scan cluster health immediately
        await checkClusterHealth();
        
    } catch (err) {
        logToConsole(`Failed to connect to ${inputUrl}: ${err.message}`, true);
        statusBadge.textContent = "Disconnected";
        statusBadge.className = "cluster-badge disconnected";
        isConnected = false;
    }
}

function updateDropdownOptions() {
    const currentSelection = targetNodeSelect.value;
    targetNodeSelect.innerHTML = '';
    
    discoveredNodes.forEach(nodeUrl => {
        const option = document.createElement('option');
        option.value = nodeUrl;
        option.textContent = nodeUrl;
        if (nodeUrl === currentSelection) {
            option.selected = true;
        }
        targetNodeSelect.appendChild(option);
    });
    
    // Default selection if nothing selected
    if (!targetNodeSelect.value && discoveredNodes.length > 0) {
        targetNodeSelect.value = discoveredNodes[0];
    }
}

// Periodic Health Checker
async function checkClusterHealth() {
    if (!isConnected) return;
    
    const nodeStatusPromises = discoveredNodes.map(async (nodeUrl) => {
        try {
            const res = await fetch(`${nodeUrl}/cluster/info`, { method: 'GET' });
            const data = await res.json();
            
            // Check if this node discovered any new nodes
            if (data.master && !discoveredNodes.includes(normalizeUrl(data.master))) {
                discoveredNodes.push(normalizeUrl(data.master));
                updateDropdownOptions();
            }
            if (data.slaves) {
                data.slaves.forEach(s => {
                    const norm = normalizeUrl(s);
                    if (!discoveredNodes.includes(norm)) {
                        discoveredNodes.push(norm);
                        updateDropdownOptions();
                    }
                });
            }

            return {
                url: nodeUrl,
                online: true,
                role: data.role
            };
        } catch (err) {
            return {
                url: nodeUrl,
                online: false,
                role: 'offline'
            };
        }
    });

    const results = await Promise.all(nodeStatusPromises);
    
    // Update our connected node's role (it might have changed due to election)
    const myNode = results.find(n => n.url === currentConnection);
    if (myNode && myNode.online) {
        connectedRole = myNode.role;
        statusBadge.textContent = `Connected (${connectedRole.toUpperCase()})`;
        updateApprovalsVisibility();
    }
    
    // Render status cards
    nodesContainer.innerHTML = '';
    results.forEach(node => {
        const card = document.createElement('div');
        card.className = `node-card ${node.online ? 'online' : 'offline'}`;
        
        // Extract port and IP
        let ipPort = node.url.replace('http://', '').replace('https://', '');
        
        card.innerHTML = `
            <div class="node-info">
                <h4>${ipPort}</h4>
                <p>Role: <span class="role-badge ${node.role}">${node.role.toUpperCase()}</span></p>
            </div>
            <div class="status-indicator">
                <span class="status-dot ${node.online ? 'green' : 'red'}"></span>
                <span class="status-text">${node.online ? 'Online' : 'Offline'}</span>
            </div>
        `;
        nodesContainer.appendChild(card);
    });
}

// API Dispatcher
async function sendRequest(endpoint, method, payload) {
    const targetUrl = targetNodeSelect.value;
    if (!targetUrl) {
        logToConsole("Error: Please connect to a node first", true);
        return;
    }
    
    const fullUrl = `${targetUrl}${endpoint}`;
    logToConsole(`Dispatched ${method} request to node: ${fullUrl}`);
    
    try {
        const res = await fetch(fullUrl, {
            method: method,
            headers: { 'Content-Type': 'application/json' },
            body: payload ? JSON.stringify(payload) : null
        });
        const data = await res.json();
        
        if (data.success) {
            logToConsole(data);
        } else {
            logToConsole(`Error: ${data.message}`, true);
        }
    } catch (err) {
        logToConsole(`Request failed. Node (${targetUrl}) might be offline. Error: ${err.message}`, true);
    }
}

// Schema Operations
function createDB() {
    const dbName = document.getElementById('db-name').value;
    if (!dbName) return alert("Please enter database name");
    sendRequest('/db/create', 'POST', { db_name: dbName });
}

// [Delete Database]
function dropDB() {
    const dbName = document.getElementById('db-name').value;
    if (!dbName) return alert("Please enter database name");
    if (!confirm(`Are you sure you want to drop database "${dbName}"?`)) return;
    sendRequest('/db/drop', 'DELETE', { db_name: dbName });
}

function createTable() {
    const dbName = document.getElementById('db-name').value;
    const tableName = document.getElementById('table-name').value;
    const colsStr = document.getElementById('table-columns').value;
    
    if (!dbName || !tableName || !colsStr) return alert("Please specify DB Name, Table Name, and Attributes");
    
    const attributes = colsStr.split(',').map(col => {
        const [name, type] = col.split(':').map(s => s.trim());
        return { name, type };
    });
    
    sendRequest('/table/create', 'POST', { db_name: dbName, table_name: tableName, attributes });
}

// [Delete Table]
function dropTable() {
    const dbName = document.getElementById('db-name').value;
    const tableName = document.getElementById('table-name').value;
    if (!dbName || !tableName) return alert("Please specify DB Name and Table Name");
    if (!confirm(`Are you sure you want to drop table "${tableName}"?`)) return;
    sendRequest('/table/drop', 'DELETE', { db_name: dbName, table_name: tableName });
}

// Data Record Operations
function insertRecord() {
    const dbName = document.getElementById('db-name').value;
    const tableName = document.getElementById('table-name').value;
    const payloadStr = document.getElementById('insert-payload').value;
    
    if (!dbName || !tableName || !payloadStr) return alert("Make sure DB Name, Table Name, and Record JSON are filled");
    
    try {
        const record = JSON.parse(payloadStr);
        sendRequest('/record/insert', 'POST', { db_name: dbName, table_name: tableName, record });
    } catch (err) {
        alert("Invalid JSON format in Insert payload. Example: {\"id\": 1, \"name\": \"Ali\"}");
    }
}

// [Select]
function selectRecords() {
    const dbName = document.getElementById('db-name').value;
    const tableName = document.getElementById('table-name').value;
    const conditionStr = document.getElementById('select-condition').value || "{}";
    
    if (!dbName || !tableName) return alert("Make sure DB Name and Table Name are filled");
    
    try {
        const condition = JSON.parse(conditionStr);
        sendRequest('/record/select', 'POST', { db_name: dbName, table_name: tableName, condition });
    } catch (err) {
        alert("Invalid JSON format in Select condition payload");
    }
}

// [Search (Substring Search)]
function searchRecords() {
    const dbName = document.getElementById('db-name').value;
    const tableName = document.getElementById('table-name').value;
    const query = document.getElementById('search-query').value;
    
    if (!dbName || !tableName) return alert("Make sure DB Name and Table Name are filled");
    
    sendRequest('/record/search', 'POST', { db_name: dbName, table_name: tableName, query: query });
}

// [Update]
function updateRecords() {
    const dbName = document.getElementById('db-name').value;
    const tableName = document.getElementById('table-name').value;
    const conditionStr = document.getElementById('update-condition').value || "{}";
    const updatesStr = document.getElementById('update-payload').value;
    
    if (!dbName || !tableName || !updatesStr) return alert("Make sure DB Name, Table Name, and Update payload are filled");
    
    try {
        const condition = JSON.parse(conditionStr);
        const updates = JSON.parse(updatesStr);
        sendRequest('/record/update', 'PUT', { db_name: dbName, table_name: tableName, condition, updates });
    } catch (err) {
        alert("Invalid JSON in Update payloads. Please check syntax");
    }
}

// [Delete Record]
function deleteRecords() {
    const dbName = document.getElementById('db-name').value;
    const tableName = document.getElementById('table-name').value;
    const conditionStr = document.getElementById('delete-condition').value || "{}";
    
    if (!dbName || !tableName) return alert("Make sure DB Name and Table Name are filled");
    if (!confirm("Are you sure you want to delete matching records?")) return;
    
    try {
        const condition = JSON.parse(conditionStr);
        sendRequest('/record/delete', 'DELETE', { db_name: dbName, table_name: tableName, condition });
    } catch (err) {
        alert("Invalid JSON in Delete condition payload");
    }
}

// [Register Slave Node]
async function registerSlaveNode() {
    const inputUrl = normalizeUrl(document.getElementById('slave-register-url').value);
    const priorityVal = document.getElementById('slave-register-priority').value;
    const priority = parseInt(priorityVal, 10) || 1;
    
    if (!inputUrl) {
        alert("Please enter slave URL");
        return;
    }
    
    logToConsole(`Registering slave ${inputUrl} with Master (priority ${priority})...`);
    
    try {
        const res = await fetch(`${currentConnection}/slave/register`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                address: inputUrl,
                priority: priority
            })
        });
        const data = await res.json();
        
        if (data.success) {
            logToConsole(`Slave registered successfully: ${inputUrl} (${data.message})`);
            document.getElementById('slave-register-url').value = '';
            document.getElementById('slave-register-priority').value = '1';
            await checkClusterHealth();
        } else {
            logToConsole(`Failed to register slave: ${data.message}`, true);
        }
    } catch (err) {
        logToConsole(`Error registering slave: ${err.message}`, true);
    }
}

// ── Master Authorization Queue Logic ──────────────────────
function updateApprovalsVisibility() {
    const panel = document.getElementById('approvals-panel');
    if (connectedRole === 'master') {
        panel.style.display = 'flex';
    } else {
        panel.style.display = 'none';
    }
}

async function fetchApprovals() {
    if (!isConnected || connectedRole !== 'master') return;
    
    try {
        const res = await fetch(`${currentConnection}/master/approvals`);
        const list = await res.json();
        renderApprovals(list);
    } catch (err) {
        // Master might be busy, silently skip
    }
}

function renderApprovals(list) {
    const container = document.getElementById('approvals-list');
    const badge = document.getElementById('approvals-count');
    
    badge.textContent = list.length;
    
    if (list.length === 0) {
        container.innerHTML = '<p class="empty-queue">No pending requests.</p>';
        return;
    }
    
    container.innerHTML = '';
    list.forEach(req => {
        // Truncate payload for display
        let shortPayload = req.payload;
        if (shortPayload && shortPayload.length > 80) {
            shortPayload = shortPayload.substring(0, 80) + '...';
        }
        
        const card = document.createElement('div');
        card.className = 'approval-card';
        card.innerHTML = `
            <div class="approval-info">
                <span class="approval-id">Request #${req.id}</span>
                <span class="approval-action">Slave :${req.slave_port} → ${req.action}</span>
                <span class="approval-detail" title="${req.payload}">${shortPayload}</span>
            </div>
            <div class="approval-actions">
                <button class="approve-btn" onclick="decideRequest('${req.id}', 'approve')">✓ Approve</button>
                <button class="reject-btn" onclick="decideRequest('${req.id}', 'reject')">✗ Reject</button>
            </div>
        `;
        container.appendChild(card);
    });
}

async function decideRequest(id, decision) {
    try {
        const res = await fetch(`${currentConnection}/master/approvals/decide`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id, decision })
        });
        const data = await res.json();
        
        if (data.success) {
            logToConsole(`Request #${id} ${decision === 'approve' ? '✅ APPROVED' : '❌ REJECTED'}`);
            // Refresh the list immediately
            await fetchApprovals();
        } else {
            logToConsole(`Failed to decide request #${id}: ${data.message}`, true);
        }
    } catch (err) {
        logToConsole(`Error deciding request: ${err.message}`, true);
    }
}

// Bootstrapping
connectToCluster(); // Try to auto-connect to default localhost:8080
setInterval(checkClusterHealth, 2000); // Check node health and topology updates every 2s
setInterval(fetchApprovals, 1500);      // Poll pending approvals every 1.5s
