const nodeIDs = [1, 2, 3];
const state = { nodes: new Map(), leaderID: null, eventLimit: 8 };

const elements = {
  nodeGrid: document.querySelector("#node-grid"),
  connection: document.querySelector("#connection-status"),
  leader: document.querySelector("#leader-summary"),
  term: document.querySelector("#term-summary"),
  updated: document.querySelector("#last-updated"),
  writeTarget: document.querySelector("#write-target"),
  writeResult: document.querySelector("#write-result"),
  events: document.querySelector("#events"),
  readResults: document.querySelector("#read-results"),
  setForm: document.querySelector("#set-form"),
  setKey: document.querySelector("#set-key"),
  setValue: document.querySelector("#set-value"),
  readForm: document.querySelector("#read-form"),
  readKey: document.querySelector("#read-key"),
  deleteButton: document.querySelector("#delete-button"),
};

function apiPath(nodeID, path) {
  return `/api/node-${nodeID}${path}`;
}

async function request(nodeID, path, options = {}) {
  try {
    // A dashboard must observe current cluster state, rather than a cached
    // /leader response from before an election or failover.
    const response = await fetch(apiPath(nodeID, path), { cache: "no-store", ...options });
    const body = await response.text();
    return { reachable: true, response, body };
  } catch (error) {
    return { reachable: false, error };
  }
}

async function inspectNode(nodeID) {
  const [health, leader] = await Promise.all([
    request(nodeID, "/health"),
    request(nodeID, "/leader"),
  ]);
  if (!health.reachable || !leader.reachable || !leader.response.ok) {
    return { id: nodeID, reachable: false, error: leader.body || health.error?.message || "unreachable" };
  }
  try {
    return { id: nodeID, reachable: health.response.ok, ...JSON.parse(leader.body) };
  } catch {
    return { id: nodeID, reachable: false, error: "invalid leader response" };
  }
}

function chooseLeader(nodes) {
  const observations = new Map();
  nodes.filter((node) => node.reachable && node.known).forEach((node) => {
    observations.set(node.leader_id, (observations.get(node.leader_id) || 0) + 1);
  });
  let chosen = null;
  let mostVotes = 0;
  for (const [id, votes] of observations) {
    if (votes > mostVotes) {
      chosen = id;
      mostVotes = votes;
    }
  }
  return chosen;
}

function renderNodes(nodes) {
  elements.nodeGrid.replaceChildren(...nodes.map((node) => {
    const card = document.createElement("article");
    const isLeader = node.id === state.leaderID;
    card.className = `node-card${isLeader ? " leader" : ""}${node.reachable ? "" : " unreachable"}`;
    const role = !node.reachable ? "OFFLINE" : isLeader ? "LEADER" : "FOLLOWER";
    const knownLeader = node.known ? `Node ${node.leader_id}` : "Unknown";
    card.innerHTML = `
      <div class="node-top">
        <span class="node-name">Node ${node.id}</span>
        <span class="node-role">${role}</span>
      </div>
      <div class="node-data"><span>HTTP process</span><strong>${node.reachable ? "reachable" : "unreachable"}</strong></div>
      <div class="node-data"><span>Current term</span><strong>${node.reachable ? node.term : "—"}</strong></div>
      <div class="node-note">${node.reachable ? `This node reports leader: ${knownLeader}.` : "No response through the dashboard proxy."}</div>
    `;
    return card;
  }));
}

function eventTime() {
  return new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(new Date());
}

function addEvent(message) {
  if (elements.events.firstElementChild?.textContent.includes("Waiting for")) {
    elements.events.replaceChildren();
  }
  const event = document.createElement("li");
  event.innerHTML = `<time>${eventTime()}</time>${message}`;
  elements.events.prepend(event);
  while (elements.events.children.length > state.eventLimit) {
    elements.events.lastElementChild.remove();
  }
}

function setWriteResult(message, kind = "") {
  elements.writeResult.textContent = message;
  elements.writeResult.className = `operation-result ${kind}`;
}

async function refreshCluster() {
  const nodes = await Promise.all(nodeIDs.map(inspectNode));
  const previousLeader = state.leaderID;
  state.nodes = new Map(nodes.map((node) => [node.id, node]));
  state.leaderID = chooseLeader(nodes);
  const reachable = nodes.filter((node) => node.reachable).length;
  const observedTerm = Math.max(0, ...nodes.filter((node) => node.reachable).map((node) => Number(node.term) || 0));

  renderNodes(nodes);
  elements.connection.className = `connection ${reachable ? "online" : "offline"}`;
  elements.connection.lastElementChild.textContent = reachable ? `${reachable}/3 node HTTP APIs reachable` : "Cluster proxy unavailable";
  elements.leader.textContent = state.leaderID ? `Node ${state.leaderID}` : "No leader observed";
  elements.term.textContent = observedTerm || "—";
  elements.updated.textContent = `Last poll ${new Date().toLocaleTimeString()}`;
  elements.writeTarget.textContent = state.leaderID ? `Writes target Node ${state.leaderID}` : "No leader available";

  if (previousLeader !== null && previousLeader !== state.leaderID) {
    addEvent(state.leaderID ? `Leader observation changed: Node ${previousLeader} → Node ${state.leaderID}.` : `Leader observation lost after Node ${previousLeader}.`);
  } else if (previousLeader === null && state.leaderID) {
    addEvent(`Observed Node ${state.leaderID} as leader in term ${observedTerm}.`);
  }
}

async function readAll(key) {
  if (!key) return;
  elements.readResults.textContent = "Reading each replica…";
  const readings = await Promise.all(nodeIDs.map(async (nodeID) => {
    const result = await request(nodeID, `/get?key=${encodeURIComponent(key)}`);
    if (!result.reachable) return { nodeID, status: "unreachable", value: "—" };
    if (result.response.status === 404) return { nodeID, status: "not found", value: "—" };
    if (!result.response.ok) return { nodeID, status: `HTTP ${result.response.status}`, value: result.body };
    return { nodeID, status: "local value", value: result.body };
  }));
  elements.readResults.replaceChildren(...readings.map((reading) => {
    const row = document.createElement("div");
    row.className = "read-row";
    const node = document.createElement("strong");
    node.textContent = `Node ${reading.nodeID}`;
    const value = document.createElement("span");
    value.className = "value";
    value.textContent = reading.value;
    const status = document.createElement("span");
    status.className = "read-status";
    status.textContent = reading.status;
    row.append(node, value, status);
    return row;
  }));
  addEvent(`Read key “${key}” from all local replicas.`);
}

async function write(method, key, value = "") {
  if (!state.leaderID) {
    setWriteResult("No leader is currently observed. Wait for election, then retry.", "error");
    return;
  }
  setWriteResult(`Sending ${method} to Node ${state.leaderID}; waiting for majority commit…`);
  const options = method === "SET"
    ? { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key, value }), redirect: "manual" }
    : { method: "DELETE", redirect: "manual" };
  const path = method === "SET" ? "/set" : `/delete?key=${encodeURIComponent(key)}`;
  const result = await request(state.leaderID, path, options);
  if (!result.reachable) {
    setWriteResult(`Node ${state.leaderID} could not be reached. Refreshing leader state.`, "error");
    addEvent(`${method} could not reach observed leader Node ${state.leaderID}.`);
  } else if (result.response.type === "opaqueredirect" || result.response.status === 307) {
    setWriteResult("Leadership changed during the request. Leader state is refreshing; retry the write.", "error");
    addEvent(`${method} observed a leader change before confirmation.`);
  } else if (!result.response.ok) {
    setWriteResult(`${method} was not confirmed: ${result.body || `HTTP ${result.response.status}`}`, "error");
    addEvent(`${method} was not confirmed by a quorum.`);
  } else {
    setWriteResult(`${method} succeeded after the leader confirmed a Raft majority.`, "success");
    addEvent(`${method} for “${key}” was quorum-acknowledged by Node ${state.leaderID}.`);
    await readAll(key);
  }
  await refreshCluster();
}

elements.setForm.addEventListener("submit", (event) => {
  event.preventDefault();
  write("SET", elements.setKey.value.trim(), elements.setValue.value);
});
elements.deleteButton.addEventListener("click", () => write("DELETE", elements.setKey.value.trim()));
elements.readForm.addEventListener("submit", (event) => {
  event.preventDefault();
  readAll(elements.readKey.value.trim());
});

refreshCluster();
window.setInterval(refreshCluster, 2000);
