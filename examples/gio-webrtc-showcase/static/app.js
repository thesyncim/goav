(() => {
  let state = null;
  let viewerId = "";
  let pc = null;
  let lastRenegSeq = -1;
  let negotiating = false;
  let lastGoCode = "";
  let lastGraphKey = "";
  let lastEventsKey = "";
  const remoteStreams = new Map(); // trackId -> MediaStream
  const trackFPS = new Map();      // trackId -> fps

  const $ = (id) => document.getElementById(id);

  async function api(method, path, body) {
    const res = await fetch(path, {
      method,
      headers: body ? { "Content-Type": "application/json" } : {},
      body: body ? JSON.stringify(body) : undefined,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      showError(data.error || `${method} ${path} failed (${res.status})`);
      throw new Error(data.error || res.statusText);
    }
    hideError();
    return data;
  }

  function showError(msg) {
    const el = $("err-banner");
    el.textContent = msg;
    el.classList.add("show");
  }
  function hideError() {
    $("err-banner").classList.remove("show");
  }

  // --- WebRTC Loopback Preview Peer ---
  async function negotiateWebRTC() {
    if (negotiating) return;
    negotiating = true;
    try {
      if (!pc || pc.connectionState === "failed" || pc.connectionState === "closed" || pc.connectionState === "disconnected") {
        if (pc) {
          try { pc.close(); } catch (_) {}
        }
        viewerId = "";
        remoteStreams.clear();
        pc = new RTCPeerConnection();
        pc.onconnectionstatechange = () => {
          if (pc && (pc.connectionState === "failed" || pc.connectionState === "disconnected" || pc.connectionState === "closed")) {
            setTimeout(() => negotiateWebRTC(), 600);
          }
        };
        pc.ontrack = (ev) => {
          const track = ev.track;
          const knownIds = (state?.branches || []).map((b) => b.id);
          let id = track.id;
          if (!knownIds.includes(id) && ev.streams?.[0]?.id && knownIds.includes(ev.streams[0].id)) {
            id = ev.streams[0].id;
          }
          if (!knownIds.includes(id) && ev.transceiver?.mid != null) {
            const idx = Number(ev.transceiver.mid);
            if (!Number.isNaN(idx) && knownIds[idx]) id = knownIds[idx];
          }
          const ms = new MediaStream([track]);
          remoteStreams.set(id, ms);
          attachVideoElement(id, ms);
        };
      }
      const offer = await api("POST", "/api/webrtc/offer", { viewerId });
      viewerId = offer.viewerId;
      await pc.setRemoteDescription({ type: "offer", sdp: offer.sdp });
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      await api("POST", "/api/webrtc/answer", { viewerId, sdp: pc.localDescription.sdp });
    } catch (e) {
      console.warn("webrtc negotiate:", e);
    } finally {
      negotiating = false;
    }
  }

  function attachVideoElement(branchId, ms) {
    const video = document.querySelector(`video[data-branch="${CSS.escape(branchId)}"]`);
    if (video && video.srcObject !== ms) {
      video.srcObject = ms;
      video.play().catch(() => {});
    }
    syncProgramVideo();
  }

  // Drive the main Program Monitor directly from the highest-resolution live
  // WebRTC stream (silky-smooth 30fps GPU decode).
  function syncProgramVideo() {
    const progVideo = $("program-video");
    if (!progVideo) return;
    const branches = state?.branches || [];
    let best = null;
    for (const b of branches) {
      if (b.paused || !remoteStreams.has(b.id)) continue;
      if (!best || b.width * b.height > best.width * best.height) best = b;
    }
    if (best) {
      const ms = remoteStreams.get(best.id);
      if (progVideo.srcObject !== ms) {
        progVideo.srcObject = ms;
        progVideo.play().catch(() => {});
      }
    } else if (progVideo.srcObject) {
      progVideo.srcObject = null;
    }
  }

  // Poll decoded WebRTC FPS per track every second.
  setInterval(async () => {
    if (!pc || pc.connectionState !== "connected") return;
    try {
      const stats = await pc.getStats();
      const knownIds = (state?.branches || []).map((b) => b.id);
      stats.forEach((rep) => {
        if (rep.type === "inbound-rtp" && rep.kind === "video") {
          let tid = rep.trackIdentifier;
          if (!knownIds.includes(tid) && rep.mid != null) {
            const idx = Number(rep.mid);
            if (!Number.isNaN(idx) && knownIds[idx]) tid = knownIds[idx];
          }
          if (tid && typeof rep.framesPerSecond === "number") {
            trackFPS.set(tid, rep.framesPerSecond);
            const badge = document.querySelector(`[data-fps="${CSS.escape(tid)}"]`);
            if (badge) badge.textContent = `${rep.framesPerSecond.toFixed(0)} fps`;
          }
        }
      });
    } catch (_) {}
  }, 1000);

  // --- SSE State Subscription ---
  function connectSSE() {
    const es = new EventSource("/api/events");
    es.onmessage = (ev) => {
      try {
        const next = JSON.parse(ev.data);
        renderState(next);
      } catch (_) {}
    };
    es.onerror = () => {
      es.close();
      lastRenegSeq = -1;
      setTimeout(connectSSE, 1200);
    };
  }

  function renderState(next) {
    state = next;

    // Top bar
    const fps = (next.program?.fps || 0).toFixed(1);
    $("stat-program").textContent = `${next.program?.width || 0}×${next.program?.height || 0} · ${fps} fps`;
    $("stat-viewers").textContent = `${next.viewers || 0} viewer${next.viewers === 1 ? "" : "s"}`;
    $("scene-pill").textContent = next.config.scene;
    $("hud-left").textContent = `PROGRAM · ${next.config.scene.toUpperCase()} · ${next.config.width}×${next.config.height}`;
    $("hud-right").textContent = `${fps} FPS · ${next.program?.frames || 0} frames`;

    $("badge-rec").classList.toggle("hidden", !next.recording?.active);
    $("badge-hls").classList.toggle("hidden", !next.hls?.active);

    if (next.lastError) showError(next.lastError);

    // Scene buttons
    document.querySelectorAll("#scene-buttons button").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.scene === next.config.scene);
    });
    $("sel-canvas").value = `${next.config.width}x${next.config.height}`;
    $("sel-pip-corner").value = next.config.pipCorner || "br";

    // Devices
    syncSelect($("sel-camera"), (next.devices?.cameras || []).map((c) => ({ value: c.id, label: c.name })), next.config.cameraId, "Default Camera");
    syncSelect($("sel-display"), (next.devices?.displays || []).map((d) => ({ value: String(d.id), label: d.name })), String(next.config.displayId || ""), "Main Display");
    $("btn-cam-perm").textContent = next.devices?.cameraAccess ? "Camera ✓" : "Grant Camera";
    $("btn-scr-perm").textContent = next.devices?.screenAccess ? "Screen ✓" : "Grant Screen";

    // Branches
    renderBranches(next.branches || []);

    // Recording
    const recBtn = $("btn-record");
    if (next.recording?.active) {
      recBtn.textContent = "■ Stop & Save";
      recBtn.className = "rec-live";
      $("rec-size").textContent = `${Math.round((next.recording.bytes || 0) / 1024)} KB`;
      $("rec-path").textContent = next.recording.path || "";
    } else {
      recBtn.textContent = "● Start Recording";
      recBtn.className = "danger";
      $("rec-size").textContent = "idle";
    }

    // HLS
    const hlsBtn = $("btn-hls");
    if (next.hls?.active) {
      hlsBtn.textContent = "Stop LL-HLS";
      hlsBtn.className = "danger";
      $("hls-badge").textContent = "live";
      $("hls-url").textContent = next.hls.url || "";
    } else {
      hlsBtn.textContent = "Start LL-HLS";
      hlsBtn.className = "";
      $("hls-badge").textContent = "off";
      $("hls-url").textContent = "";
    }

    // WHIP
    const whipBtn = $("btn-whip");
    if (next.whip?.active) {
      whipBtn.textContent = "Stop WHIP";
      whipBtn.className = "danger";
      $("whip-badge").textContent = "live";
    } else {
      whipBtn.textContent = "Publish WHIP";
      whipBtn.className = "";
      $("whip-badge").textContent = "off";
    }

    // Live Code & Graph Inspector (only re-render DOM when content actually changes)
    $("line-count").textContent = `${next.code?.lineCount || 0} lines of Go`;
    const goCode = next.code?.goCode || "";
    if (goCode !== lastGoCode) {
      lastGoCode = goCode;
      $("go-code").innerHTML = highlightGo(goCode);
      $("cli-code").textContent = next.code?.cliRecipe || "";
      $("mermaid-code").textContent = next.code?.mermaid || "";
    }
    const graphKey = JSON.stringify(next.graph || {});
    if (graphKey !== lastGraphKey) {
      lastGraphKey = graphKey;
      renderGraphSVG(next.graph);
    }
    const evKey = `${(next.events || []).length}:${(next.events || []).at(-1)?.seq || 0}`;
    if (evKey !== lastEventsKey) {
      lastEventsKey = evKey;
      renderEvents(next.events || []);
    }

    // Renegotiate WebRTC whenever branch set changes or on first load.
    if (next.renegSeq !== lastRenegSeq && (next.branches || []).length > 0) {
      lastRenegSeq = next.renegSeq;
      negotiateWebRTC();
    }
  }

  function syncSelect(el, items, current, fallbackLabel) {
    const key = items.map((i) => `${i.value}:${i.label}`).join("|");
    if (el.dataset.key === key) return;
    el.dataset.key = key;
    el.innerHTML = "";
    if (items.length === 0) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = fallbackLabel;
      el.appendChild(opt);
      return;
    }
    for (const item of items) {
      const opt = document.createElement("option");
      opt.value = item.value;
      opt.textContent = item.label;
      if (item.value === current) opt.selected = true;
      el.appendChild(opt);
    }
  }

  function renderBranches(branches) {
    const grid = $("branch-grid");
    const existingIds = new Set(Array.from(grid.children).map((c) => c.dataset.id));
    const nextIds = new Set(branches.map((b) => b.id));

    for (const child of Array.from(grid.children)) {
      if (!nextIds.has(child.dataset.id)) child.remove();
    }

    for (const b of branches) {
      let card = grid.querySelector(`.branch-card[data-id="${CSS.escape(b.id)}"]`);
      if (!card) {
        card = document.createElement("div");
        card.className = "branch-card";
        card.dataset.id = b.id;
        card.innerHTML = `
          <div class="card-head">
            <strong class="mono">${escapeHTML(b.id)}</strong>
            <span class="pill cyan">${escapeHTML(b.codec.toUpperCase())}</span>
          </div>
          <div class="branch-video">
            <video data-branch="${escapeHTML(b.id)}" autoplay playsinline muted></video>
            <span class="v-badge" data-fps="${escapeHTML(b.id)}">-- fps</span>
          </div>
          <div class="branch-meta">
            <span data-dim></span>
            <span data-rate></span>
          </div>
          <div class="slider-row">
            <span class="muted">Bitrate</span>
            <input type="range" min="150000" max="2500000" step="50000" value="${b.bitrate}" data-bitrate />
            <span class="mono" data-br-label style="min-width:56px;text-align:right;"></span>
          </div>
          <div class="btn-row">
            <button class="sm" data-toggle-pause></button>
            <button class="sm" data-rebranch="640x360">360p</button>
            <button class="sm" data-rebranch="960x540">540p</button>
            <button class="sm danger" data-remove style="margin-left:auto;">Detach</button>
          </div>
        `;
        const slider = card.querySelector("[data-bitrate]");
        slider.addEventListener("change", () => {
          api("POST", `/api/branches/${encodeURIComponent(b.id)}/bitrate`, { bitrate: Number(slider.value) });
        });
        slider.addEventListener("input", () => {
          card.querySelector("[data-br-label]").textContent = `${Math.round(Number(slider.value) / 1000)}k`;
        });
        card.querySelector("[data-toggle-pause]").addEventListener("click", () => {
          const cur = state?.branches?.find((x) => x.id === b.id);
          const action = cur?.paused ? "resume" : "pause";
          api("POST", `/api/branches/${encodeURIComponent(b.id)}/${action}`);
        });
        card.querySelectorAll("[data-rebranch]").forEach((rb) => {
          rb.addEventListener("click", () => {
            const [w, h] = rb.dataset.rebranch.split("x").map(Number);
            const cur = state?.branches?.find((x) => x.id === b.id);
            api("POST", `/api/branches/${encodeURIComponent(b.id)}/rebranch`, { width: w, height: h, bitrate: cur?.bitrate || b.bitrate });
          });
        });
        card.querySelector("[data-remove]").addEventListener("click", () => {
          api("DELETE", `/api/branches/${encodeURIComponent(b.id)}`);
        });
        grid.appendChild(card);
      }

      card.querySelector("[data-dim]").textContent = `${b.width}×${b.height} · ${b.packets} pkts`;
      card.querySelector("[data-rate]").textContent = `${(b.kbps || 0).toFixed(0)} kbps`;
      card.querySelector("[data-br-label]").textContent = `${Math.round(b.bitrate / 1000)}k`;
      const pauseBtn = card.querySelector("[data-toggle-pause]");
      pauseBtn.textContent = b.paused ? "Resume" : "Pause";
      pauseBtn.classList.toggle("active", b.paused);

      if (remoteStreams.has(b.id)) {
        attachVideoElement(b.id, remoteStreams.get(b.id));
      }
    }
    syncProgramVideo();
  }

  // --- Interactive SVG DAG Renderer ---
  function renderGraphSVG(graph) {
    const wrap = $("graph-svg-wrap");
    const nodes = graph?.nodes || [];
    const edges = graph?.edges || [];
    if (nodes.length === 0) {
      wrap.innerHTML = `<div class="muted">No active pipeline graph</div>`;
      return;
    }

    // Topological layers via BFS from roots
    const inDeg = new Map(nodes.map((n) => [n.name, 0]));
    const adj = new Map(nodes.map((n) => [n.name, []]));
    for (const e of edges) {
      if (adj.has(e.from)) adj.get(e.from).push(e.to);
      if (inDeg.has(e.to)) inDeg.set(e.to, inDeg.get(e.to) + 1);
    }
    const level = new Map();
    const queue = [];
    for (const n of nodes) {
      if ((inDeg.get(n.name) || 0) === 0) {
        level.set(n.name, 0);
        queue.push(n.name);
      }
    }
    const maxSteps = Math.max(64, nodes.length * nodes.length);
    let steps = 0;
    while (queue.length && steps < maxSteps) {
      steps++;
      const u = queue.shift();
      const l = level.get(u) || 0;
      if (l >= nodes.length) continue;
      for (const v of adj.get(u) || []) {
        const nextL = l + 1;
        if (nextL > (level.get(v) ?? -1) && nextL < nodes.length) {
          level.set(v, nextL);
          queue.push(v);
        }
      }
    }
    const layers = new Map();
    for (const n of nodes) {
      const l = level.get(n.name) || 0;
      if (!layers.has(l)) layers.set(l, []);
      layers.get(l).push(n);
    }

    const colW = 158, rowH = 58, boxW = 136, boxH = 40, pad = 16;
    const pos = new Map();
    let maxCols = 0, maxRows = 0;
    for (const [l, list] of layers.entries()) {
      maxCols = Math.max(maxCols, l + 1);
      maxRows = Math.max(maxRows, list.length);
      list.forEach((n, idx) => {
        pos.set(n.name, { x: pad + l * colW, y: pad + idx * rowH });
      });
    }
    const svgW = Math.max(360, pad * 2 + maxCols * colW);
    const svgH = Math.max(180, pad * 2 + maxRows * rowH);

    const colorFor = (kind) => {
      if (kind === "source") return "#10b981";
      if (kind === "sink") return "#f59e0b";
      if (kind === "tap") return "#38bdf8";
      return "#a78bfa";
    };

    let svg = `<svg width="${svgW}" height="${svgH}" viewBox="0 0 ${svgW} ${svgH}" xmlns="http://www.w3.org/2000/svg">`;
    for (const e of edges) {
      const a = pos.get(e.from), b = pos.get(e.to);
      if (!a || !b) continue;
      const x1 = a.x + boxW, y1 = a.y + boxH / 2;
      const x2 = b.x, y2 = b.y + boxH / 2;
      const mx = (x1 + x2) / 2;
      svg += `<path d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}" fill="none" stroke="#334155" stroke-width="1.6"/>`;
    }
    for (const n of nodes) {
      const p = pos.get(n.name);
      const col = colorFor(n.kind);
      const label = n.name.length > 18 ? n.name.slice(0, 17) + "…" : n.name;
      svg += `<g transform="translate(${p.x},${p.y})">
        <rect width="${boxW}" height="${boxH}" rx="7" fill="#131826" stroke="${col}" stroke-width="1.3"/>
        <text x="8" y="16" fill="#e2e8f0" font-size="10.5" font-family="ui-monospace,monospace" font-weight="600">${escapeHTML(label)}</text>
        <text x="8" y="30" fill="${col}" font-size="9.5" font-family="ui-monospace,monospace">${escapeHTML(n.kind)}</text>
      </g>`;
    }
    svg += `</svg>`;
    wrap.innerHTML = svg;
  }

  function renderEvents(events) {
    const list = $("events-list");
    list.innerHTML = events
      .slice()
      .reverse()
      .map(
        (e) =>
          `<div class="ev-row ${e.level === "error" ? "error" : ""}">
            <span class="ev-time">${escapeHTML(e.time)}</span>
            <span class="ev-kind">${escapeHTML(e.kind)}</span>
            <span>${escapeHTML(e.message)}</span>
          </div>`
      )
      .join("");
  }

  function highlightGo(src) {
    const escaped = escapeHTML(src);
    const re = /(\/\/[^\n]*)|(&quot;[^&]*?&quot;)|\b(go|func|var|const|return|if|else)\b|\b(goav|cameraav|screenav|codec|flow|hls)(?=\.)/g;
    return escaped.replace(re, (m, cmt, str, kw, pkg) => {
      if (cmt) return `<span class="tok-cmt">${cmt}</span>`;
      if (str) return `<span class="tok-str">${str}</span>`;
      if (kw) return `<span class="tok-kw">${kw}</span>`;
      if (pkg) return `<span class="tok-fn">${pkg}</span>`;
      return m;
    });
  }

  function escapeHTML(str) {
    return String(str).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }

  // --- Wire UI Events ---
  function sendConfigPatch(patch) {
    if (!state) return;
    api("POST", "/api/config", { ...state.config, ...patch });
  }

  document.querySelectorAll("#scene-buttons button").forEach((btn) => {
    btn.addEventListener("click", () => sendConfigPatch({ scene: btn.dataset.scene }));
  });
  $("sel-canvas").addEventListener("change", (e) => {
    const [width, height] = e.target.value.split("x").map(Number);
    sendConfigPatch({ width, height });
  });
  $("sel-pip-corner").addEventListener("change", (e) => sendConfigPatch({ pipCorner: e.target.value }));
  $("sel-camera").addEventListener("change", (e) => sendConfigPatch({ cameraId: e.target.value }));
  $("sel-display").addEventListener("change", (e) => sendConfigPatch({ displayId: Number(e.target.value) || 0 }));

  $("btn-cam-perm").addEventListener("click", async () => {
    await api("POST", "/api/permissions/camera");
    if (state?.config?.scene === "pattern") sendConfigPatch({ scene: "camera" });
  });
  $("btn-scr-perm").addEventListener("click", async () => {
    const res = await api("POST", "/api/permissions/screen");
    if (res.granted && state?.config?.scene === "pattern") sendConfigPatch({ scene: "screen" });
  });

  document.querySelectorAll("[data-add-branch]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const [codec, w, h, br] = btn.dataset.addBranch.split(":");
      api("POST", "/api/branches", { codec, width: Number(w), height: Number(h), fps: 30, bitrate: Number(br) });
    });
  });

  $("btn-record").addEventListener("click", async () => {
    try {
      if (state?.recording?.active) {
        const res = await api("POST", "/api/record/stop");
        if (res?.recording?.path) {
          $("rec-path").textContent = `Saved: ${res.recording.path} (${Math.round((res.recording.bytes || 0) / 1024)} KB)`;
        }
      } else {
        await api("POST", "/api/record/start", {
          format: $("rec-format").value,
          codec: $("rec-codec").value,
          bitrate: 1800000,
        });
      }
    } catch (e) {
      $("rec-path").textContent = `Error: ${e.message}`;
    }
  });

  $("btn-hls").addEventListener("click", () => {
    api("POST", "/api/hls", { enable: !state?.hls?.active, bitrate: 1200000 });
  });

  $("btn-whip").addEventListener("click", () => {
    if (state?.whip?.active) {
      api("POST", "/api/whip/stop");
    } else {
      api("POST", "/api/whip/start", {
        url: $("whip-url").value.trim(),
        codec: $("whip-codec").value,
        bitrate: 1200000,
      });
    }
  });

  $("btn-copy-code").addEventListener("click", () => {
    navigator.clipboard?.writeText(state?.code?.goCode || "");
    $("btn-copy-code").textContent = "Copied!";
    setTimeout(() => ($("btn-copy-code").textContent = "Copy Code"), 1200);
  });

  $("btn-copy-watch").addEventListener("click", () => {
    navigator.clipboard?.writeText(state?.watchUrl || `${location.origin}/watch`);
    $("btn-copy-watch").textContent = "Copied!";
    setTimeout(() => ($("btn-copy-watch").textContent = "Copy /watch URL"), 1200);
  });

  $("btn-open-watch").addEventListener("click", () => {
    api("POST", "/api/open-watch").catch(() => window.open("/watch", "_blank"));
  });

  document.querySelectorAll(".tab").forEach((t) => {
    t.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((x) => x.classList.remove("active"));
      t.classList.add("active");
      ["code", "graph", "events"].forEach((name) => {
        $("pane-" + name).classList.toggle("hidden", name !== t.dataset.tab);
      });
    });
  });

  // Fetch initial state immediately, then keep synced via SSE + WebRTC.
  fetch("/api/state", { cache: "no-store" }).then((r) => r.json()).then(renderState).catch(() => {});
  connectSSE();
})();
