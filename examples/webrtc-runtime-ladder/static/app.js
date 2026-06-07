const state = {
  pc: null,
  sessionID: "",
  events: null,
  remoteTracks: new Map(),
  last: null,
  synthetic: null,
  poller: null
};

const defaultReceiveKinds = ["video", "video"];

const els = {
  start: document.querySelector("#start"),
  add: document.querySelector("#add"),
  inputSource: document.querySelector("#inputSource"),
  uploadCodec: document.querySelector("#uploadCodec"),
  newKind: document.querySelector("#newKind"),
  newCodec: document.querySelector("#newCodec"),
  newWidth: document.querySelector("#newWidth"),
  newHeight: document.querySelector("#newHeight"),
  newBitrate: document.querySelector("#newBitrate"),
  local: document.querySelector("#local"),
  remotes: document.querySelector("#remotes"),
  runtimeList: document.querySelector("#runtimeList"),
  debugList: document.querySelector("#debugList"),
  eventList: document.querySelector("#eventList"),
  status: document.querySelector("#status"),
  dot: document.querySelector("#dot"),
  streamStatus: document.querySelector("#streamStatus"),
  revision: document.querySelector("#revision"),
  inboundCodec: document.querySelector("#inboundCodec"),
  branchCount: document.querySelector("#branchCount"),
  remoteCount: document.querySelector("#remoteCount"),
  videoGraph: document.querySelector("#videoGraph"),
  audioGraph: document.querySelector("#audioGraph"),
  graphBadge: document.querySelector("#graphBadge"),
  debugTotals: document.querySelector("#debugTotals"),
  eventCount: document.querySelector("#eventCount"),
  log: document.querySelector("#log")
};

initCodecControls();
els.start.addEventListener("click", () => start().catch(showError));
els.add.addEventListener("click", () => addBranch().catch(showError));
els.newKind.addEventListener("change", syncNewCodecOptions);

function initCodecControls() {
  const capabilities = RTCRtpSender.getCapabilities?.("video")?.codecs || [];
  const names = [...new Set(capabilities.map(c => codecName(c.mimeType)).filter(c => ["vp8", "vp9", "av1"].includes(c)))];
  const upload = names.length ? names : ["vp8"];
  els.uploadCodec.innerHTML = upload.map(c => `<option value="${c}">${c}</option>`).join("");
  syncNewCodecOptions();
  renderEmptyState();
}

function syncNewCodecOptions() {
  if (els.newKind.value === "audio") {
    els.newCodec.innerHTML = `<option value="opus">opus</option>`;
    els.newWidth.disabled = true;
    els.newHeight.disabled = true;
    els.newBitrate.value = "64000";
  } else {
    els.newCodec.innerHTML = `<option value="vp8">vp8</option><option value="vp9">vp9</option>`;
    els.newWidth.disabled = false;
    els.newHeight.disabled = false;
    if (Number(els.newBitrate.value) < 100000) els.newBitrate.value = "1200000";
  }
}

async function start() {
  setStatus("starting", "warn");
  els.start.disabled = true;
  const stream = await openInputStream();
  els.local.srcObject = stream;
  playMedia(els.local);

  const pc = new RTCPeerConnection({ iceServers: [] });
  state.pc = pc;
  pc.ontrack = onRemoteTrack;
  pc.onconnectionstatechange = () => {
    const status = pc.connectionState;
    setStatus(status, status === "connected" ? "live" : status === "failed" ? "bad" : "warn");
  };

  const videoTrack = stream.getVideoTracks()[0];
  const audioTrack = stream.getAudioTracks()[0];
  const videoTx = pc.addTransceiver(videoTrack, { direction: "sendonly" });
  preferCodec(videoTx, els.uploadCodec.value);
  if (audioTrack) {
    const audioTx = pc.addTransceiver(audioTrack, { direction: "sendonly" });
    preferCodec(audioTx, "opus");
  }
  reserveReceiveSlots(defaultReceiveKinds);

  const answer = await negotiate("/api/offer");
  state.sessionID = answer.id;
  connectEvents();
  startPolling();
  els.add.disabled = false;
  setStatus("negotiated", "warn");
  await refreshState();
}

async function openInputStream() {
  if (state.synthetic?.stop) state.synthetic.stop();
  if (els.inputSource.value === "synthetic") {
    return createSyntheticStream("synthetic source");
  }
  try {
    return await navigator.mediaDevices.getUserMedia({
      video: { width: { ideal: 1280 }, height: { ideal: 720 }, frameRate: { ideal: 30 } },
      audio: true
    });
  } catch (err) {
    els.log.textContent = `${err.message || err}; using synthetic source`;
    els.inputSource.value = "synthetic";
    return createSyntheticStream("camera permission fallback");
  }
}

function createSyntheticStream(label) {
  const canvas = document.createElement("canvas");
  canvas.width = 1280;
  canvas.height = 720;
  const ctx = canvas.getContext("2d");
  let frame = 0;
  let stopped = false;
  const startedAt = performance.now();

  function draw(now) {
    if (stopped) return;
    const t = (now - startedAt) / 1000;
    const hue = Math.round((t * 44) % 360);
    ctx.fillStyle = `hsl(${hue}, 38%, 12%)`;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    ctx.fillStyle = "#34d0b6";
    const x = 120 + (Math.sin(t * 1.4) + 1) * 470;
    const y = 120 + (Math.cos(t * 1.1) + 1) * 210;
    ctx.fillRect(x, y, 220, 150);

    ctx.fillStyle = "#63a4ff";
    ctx.beginPath();
    ctx.arc(920 + Math.sin(t * 1.9) * 120, 360 + Math.cos(t * 1.7) * 120, 86, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle = "#eef3f4";
    ctx.font = "700 54px system-ui, sans-serif";
    ctx.fillText("goav WebRTC runtime ladder", 58, 96);
    ctx.font = "32px system-ui, sans-serif";
    ctx.fillText(label, 60, 152);
    ctx.font = "28px ui-monospace, SFMono-Regular, Menlo, monospace";
    ctx.fillText(`frame ${frame++}`, 60, 640);
    ctx.fillText(new Date().toLocaleTimeString(), 60, 682);
    requestAnimationFrame(draw);
  }
  requestAnimationFrame(draw);

  const stream = canvas.captureStream(30);
  const videoTrack = stream.getVideoTracks()[0];
  if (videoTrack) videoTrack.contentHint = "motion";

  const AudioContextClass = window.AudioContext || window.webkitAudioContext;
  let audioContext;
  if (AudioContextClass) {
    audioContext = new AudioContextClass();
    const oscillator = audioContext.createOscillator();
    const gain = audioContext.createGain();
    const destination = audioContext.createMediaStreamDestination();
    oscillator.frequency.value = 220;
    gain.gain.value = 0.015;
    oscillator.connect(gain).connect(destination);
    oscillator.start();
    destination.stream.getAudioTracks().forEach(track => stream.addTrack(track));
  }

  state.synthetic = {
    stop() {
      stopped = true;
      stream.getTracks().forEach(track => track.stop());
      if (audioContext) audioContext.close();
    }
  };
  return stream;
}

async function negotiate(url) {
  const offer = await state.pc.createOffer();
  await state.pc.setLocalDescription(offer);
  await waitForIceGathering(state.pc);
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(state.pc.localDescription)
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "signaling failed");
  await state.pc.setRemoteDescription({ type: payload.type, sdp: payload.sdp });
  return payload;
}

function connectEvents() {
  if (state.events) state.events.close();
  const events = new EventSource(`/api/sessions/${state.sessionID}/events`);
  state.events = events;
  events.addEventListener("state", event => {
    renderState(JSON.parse(event.data));
  });
  events.onopen = () => {
    els.streamStatus.textContent = "events live";
    els.streamStatus.className = "badge live";
  };
  events.onerror = () => {
    els.streamStatus.textContent = "events reconnecting";
    els.streamStatus.className = "badge warn";
  };
}

function startPolling() {
  if (state.poller) clearInterval(state.poller);
  state.poller = setInterval(() => {
    refreshState().catch(err => console.warn(err));
  }, 1000);
}

async function addBranch() {
  if (!state.sessionID) return;
  const spec = readNewSpec();
  const response = await fetch(`/api/sessions/${state.sessionID}/branches`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(spec)
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "add failed");
  if (payload.needsRenegotiate) {
    reserveReceiveSlots([payload.branch.kind]);
    await negotiate(`/api/sessions/${state.sessionID}/offer`);
  }
  await refreshState();
}

async function updateBranch(id) {
  const row = document.querySelector(`[data-row="${id}"]`);
  const spec = {
    id,
    kind: row.dataset.kind,
    codec: row.dataset.codec,
    width: Number(row.querySelector("[data-field=width]")?.value || 0),
    height: Number(row.querySelector("[data-field=height]")?.value || 0),
    bitrate: Number(row.querySelector("[data-field=bitrate]").value)
  };
  const response = await fetch(`/api/sessions/${state.sessionID}/branches/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(spec)
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "update failed");
  await refreshState();
}

async function deleteBranch(id) {
  const response = await fetch(`/api/sessions/${state.sessionID}/branches/${id}`, { method: "DELETE" });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "delete failed");
  if (payload.needsRenegotiate) {
    await negotiate(`/api/sessions/${state.sessionID}/offer`);
  }
  document.querySelector(`[data-tile="${id}"]`)?.remove();
  await refreshState();
}

async function refreshState() {
  if (!state.sessionID) return;
  const response = await fetch(`/api/sessions/${state.sessionID}/state`);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "state failed");
  renderState(payload);
}

function readNewSpec() {
  const kind = els.newKind.value;
  return {
    kind,
    codec: els.newCodec.value,
    width: kind === "video" ? Number(els.newWidth.value) : 0,
    height: kind === "video" ? Number(els.newHeight.value) : 0,
    bitrate: Number(els.newBitrate.value)
  };
}

function onRemoteTrack(event) {
  const track = event.track;
  const id = track.id;
  state.remoteTracks.set(id, track);
  let tile = document.querySelector(`[data-tile="${id}"]`);
  if (!tile) {
    tile = document.createElement("div");
    tile.className = "tile";
    tile.dataset.tile = id;
    tile.innerHTML = `
      <div class="tile-title"><span>${escapeHTML(id)}</span><span class="badge">${track.kind}</span></div>
      <div class="tile-body"></div>
    `;
    els.remotes.append(tile);
  }
  const body = tile.querySelector(".tile-body");
  const mediaEl = document.createElement(track.kind === "audio" ? "audio" : "video");
  mediaEl.autoplay = true;
  mediaEl.playsInline = true;
  mediaEl.controls = track.kind === "audio";
  mediaEl.muted = track.kind === "video";
  mediaEl.srcObject = new MediaStream([track]);
  mediaEl.addEventListener("loadedmetadata", () => playMedia(mediaEl), { once: true });
  playMedia(mediaEl);
  body.replaceChildren(mediaEl);
  track.onended = () => {
    state.remoteTracks.delete(id);
    tile.remove();
    updateRemoteCount();
  };
  updateRemoteCount();
}

function reserveReceiveSlots(kinds) {
  if (!state.pc) return;
  for (const kind of kinds) {
    state.pc.addTransceiver(kind, { direction: "recvonly" });
  }
}

function playMedia(mediaEl) {
  const play = mediaEl.play?.();
  if (play?.catch) play.catch(() => {});
}

function renderState(payload) {
  state.last = payload;
  els.revision.textContent = `rev ${payload.revision || 0}`;
  els.inboundCodec.textContent = [payload.videoCodec, payload.audioCodec].filter(Boolean).join(" + ") || "waiting";
  els.log.textContent = payload.lastError || "";
  renderRuntimeList(payload.branches || []);
  renderDebug(payload.debug || {});
  renderEvents(payload.events || []);
  renderGraph(els.videoGraph, payload.videoGraph, "video");
  renderGraph(els.audioGraph, payload.audioGraph, "audio");
  const nodes = (payload.videoGraph?.stats?.nodeCount || 0) + (payload.audioGraph?.stats?.nodeCount || 0);
  const edges = (payload.videoGraph?.stats?.edgeCount || 0) + (payload.audioGraph?.stats?.edgeCount || 0);
  els.graphBadge.textContent = `${nodes} nodes / ${edges} edges`;
  updateRemoteCount();
}

function renderRuntimeList(branches) {
  els.branchCount.textContent = `${branches.length} branches`;
  if (!branches.length) {
    els.runtimeList.innerHTML = `<div class="empty">waiting</div>`;
    return;
  }
  els.runtimeList.innerHTML = "";
  for (const r of branches) {
    const row = document.createElement("div");
    row.className = "runtime-row";
    row.dataset.row = r.id;
    row.dataset.kind = r.kind;
    row.dataset.codec = r.codec;
    const isVideo = r.kind === "video";
    row.innerHTML = `
      <div><span class="badge">${r.codec}</span><div class="metric">${escapeHTML(r.id)}</div></div>
      <div class="metric"><strong>${r.bound ? "attached" : "waiting"}</strong><br>${r.packets} packets<br>${formatBytes(r.bytes)}</div>
      <label>Width<input data-field="width" type="number" min="64" step="2" value="${r.width || ""}" ${isVideo ? "" : "disabled"}></label>
      <label>Height<input data-field="height" type="number" min="64" step="2" value="${r.height || ""}" ${isVideo ? "" : "disabled"}></label>
      <label>Bitrate<input data-field="bitrate" type="number" min="16000" step="10000" value="${r.bitrate}"></label>
      <button data-action="apply">Apply</button>
      <button class="danger" data-action="remove">Remove</button>
    `;
    row.querySelector("[data-action=apply]").onclick = () => updateBranch(r.id).catch(showError);
    row.querySelector("[data-action=remove]").onclick = () => deleteBranch(r.id).catch(showError);
    els.runtimeList.append(row);
  }
}

function renderDebug(debug) {
  const totals = debug.totals || {};
  els.debugTotals.textContent = `${totals.packets || 0} packets / ${formatBytes(totals.bytes || 0)}`;
  const tasks = debug.tasks || [];
  if (!tasks.length) {
    els.debugList.innerHTML = `<div class="empty">waiting</div>`;
    return;
  }
  els.debugList.innerHTML = "";
  for (const task of tasks) {
    const row = document.createElement("div");
    row.className = "debug-row";
    const graph = task.graph || {};
    row.innerHTML = `
      <div>
        <span class="badge ${task.state === "running" ? "live" : "warn"}">${task.kind}</span>
        <div class="metric">${task.codec || "no track"}</div>
      </div>
      <div class="metric">
        <strong>${task.state}</strong><br>
        ${graph.nodeCount || 0} nodes, ${graph.edgeCount || 0} edges<br>
        ${task.packets || 0} packets, ${formatBytes(task.bytes || 0)}
        ${renderChips("attached", task.attached)}
        ${renderChips("waiting", task.waiting)}
      </div>
      <div class="metric">
        sources ${listCount(graph.sources)}<br>
        sinks ${listCount(graph.sinks)}
      </div>
    `;
    els.debugList.append(row);
  }
}

function renderChips(label, values) {
  if (!values?.length) return "";
  return `<div class="chips"><span class="chip">${label}</span>${values.map(v => `<span class="chip">${escapeHTML(v)}</span>`).join("")}</div>`;
}

function renderEvents(events) {
  els.eventCount.textContent = `${events.length} events`;
  if (!events.length) {
    els.eventList.innerHTML = `<div class="empty">waiting</div>`;
    return;
  }
  const visible = [...events].reverse().slice(0, 28);
  els.eventList.innerHTML = "";
  for (const event of visible) {
    const row = document.createElement("div");
    row.className = `event-row ${event.level || ""}`;
    const scope = [event.stream, event.branch].filter(Boolean).join(" / ");
    row.innerHTML = `
      <div class="metric">#${event.seq}</div>
      <div><span class="badge ${event.level === "error" ? "bad" : event.level === "debug" ? "warn" : ""}">${escapeHTML(event.kind || "event")}</span></div>
      <div class="metric"><strong>${escapeHTML(event.message || "")}</strong><br>${timeLabel(event.time)} ${escapeHTML(scope)}</div>
    `;
    els.eventList.append(row);
  }
}

function renderGraph(svg, graph, label) {
  const nodes = graph?.nodes || [];
  const edges = graph?.edges || [];
  if (!nodes.length) {
    svg.setAttribute("viewBox", "0 0 920 260");
    svg.style.height = "260px";
    svg.innerHTML = `<text x="460" y="134" text-anchor="middle" fill="#66747a">${label} graph waiting</text>`;
    return;
  }
  const depth = new Map(nodes.map(n => [n.name, 0]));
  for (let pass = 0; pass < nodes.length; pass++) {
    for (const edge of edges) {
      depth.set(edge.to, Math.max(depth.get(edge.to) || 0, (depth.get(edge.from) || 0) + 1));
    }
  }
  const lanes = new Map();
  for (const node of nodes) {
    const d = depth.get(node.name) || 0;
    if (!lanes.has(d)) lanes.set(d, []);
    lanes.get(d).push(node);
  }
  const maxDepth = Math.max(...depth.values(), 1);
  const maxLane = Math.max(...[...lanes.values()].map(lane => lane.length), 1);
  const height = Math.max(260, 62 + maxLane * 68);
  svg.setAttribute("viewBox", `0 0 1080 ${height}`);
  svg.style.height = `${Math.min(520, height)}px`;

  const pos = new Map();
  for (const [d, lane] of lanes) {
    lane.sort((a, b) => a.name.localeCompare(b.name));
    lane.forEach((node, i) => {
      const x = 24 + d * (900 / maxDepth);
      const y = 28 + i * 68;
      pos.set(node.name, { x, y });
    });
  }
  const lines = edges.map(edge => {
    const a = pos.get(edge.from);
    const b = pos.get(edge.to);
    if (!a || !b) return "";
    return `<path d="M ${a.x + 152} ${a.y + 20} C ${a.x + 212} ${a.y + 20}, ${b.x - 60} ${b.y + 20}, ${b.x} ${b.y + 20}" stroke="#3c4a52" fill="none" stroke-width="2"/>`;
  }).join("");
  const boxes = nodes.map(node => {
    const p = pos.get(node.name);
    const color = node.kind === "source" ? "#17443d" : node.kind === "sink" ? "#4a3820" : "#1d2c42";
    const title = clip(node.name, 22);
    const detail = clip(node.detail || node.kind || "", 30);
    return `<g>
      <rect x="${p.x}" y="${p.y}" width="152" height="44" rx="6" fill="${color}" stroke="#52606a"/>
      <text x="${p.x + 8}" y="${p.y + 18}" fill="#edf4f5" font-size="12">${escapeHTML(title)}</text>
      <text x="${p.x + 8}" y="${p.y + 34}" fill="#9fb0b7" font-size="10">${escapeHTML(detail)}</text>
    </g>`;
  }).join("");
  svg.innerHTML = lines + boxes;
}

function renderEmptyState() {
  renderRuntimeList([]);
  renderDebug({});
  renderEvents([]);
  renderGraph(els.videoGraph, null, "video");
  renderGraph(els.audioGraph, null, "audio");
}

function preferCodec(transceiver, codec) {
  const kind = codec === "opus" ? "audio" : "video";
  const capabilities = RTCRtpSender.getCapabilities(kind);
  if (!capabilities?.codecs?.length || !transceiver.setCodecPreferences) return;
  const preferred = capabilities.codecs.filter(c => codecName(c.mimeType) === codec);
  const rest = capabilities.codecs.filter(c => codecName(c.mimeType) !== codec);
  if (preferred.length) transceiver.setCodecPreferences([...preferred, ...rest]);
}

function waitForIceGathering(pc) {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise(resolve => {
    const done = () => {
      if (pc.iceGatheringState === "complete") {
        pc.removeEventListener("icegatheringstatechange", done);
        resolve();
      }
    };
    pc.addEventListener("icegatheringstatechange", done);
    setTimeout(resolve, 1500);
  });
}

function codecName(mime) {
  return String(mime || "").split("/").pop().toLowerCase();
}

function formatBytes(bytes) {
  if (bytes > 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`;
  if (bytes > 1000) return `${(bytes / 1000).toFixed(1)} KB`;
  return `${bytes || 0} B`;
}

function updateRemoteCount() {
  els.remoteCount.textContent = `${state.remoteTracks.size} tracks`;
}

function setStatus(text, mode) {
  els.status.textContent = text;
  els.dot.className = `dot ${mode === "live" ? "live" : mode === "bad" ? "bad" : ""}`;
}

function showError(err) {
  console.error(err);
  els.log.textContent = err.message || String(err);
  setStatus("error", "bad");
  if (!state.sessionID) els.start.disabled = false;
}

function listCount(values) {
  return values?.length ? values.length : 0;
}

function clip(value, length) {
  const text = String(value || "");
  return text.length > length ? `${text.slice(0, length - 1)}...` : text;
}

function timeLabel(value) {
  if (!value) return "";
  return new Date(value).toLocaleTimeString();
}

function escapeHTML(value) {
  return String(value).replace(/[&<>"']/g, ch => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  })[ch]);
}
