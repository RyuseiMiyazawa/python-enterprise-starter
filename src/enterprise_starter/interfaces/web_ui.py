from __future__ import annotations


def render_dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Workflow Control Plane</title>
  <style>
    :root {
      --bg: #f3efe6;
      --panel: rgba(255,255,255,0.78);
      --ink: #1f2937;
      --muted: #5b6470;
      --accent: #0f766e;
      --danger: #b91c1c;
      --line: rgba(31,41,55,0.1);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Avenir Next", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.15), transparent 30%),
        radial-gradient(circle at bottom right, rgba(217,119,6,0.12), transparent 35%),
        var(--bg);
    }
    .shell {
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    .hero {
      display: grid;
      gap: 14px;
      margin-bottom: 24px;
    }
    .hero h1 {
      margin: 0;
      font-size: clamp(2.2rem, 4vw, 4.5rem);
      line-height: 0.95;
      letter-spacing: -0.04em;
    }
    .hero p { margin: 0; color: var(--muted); max-width: 760px; }
    .grid {
      display: grid;
      grid-template-columns: 1.05fr 0.95fr;
      gap: 18px;
    }
    .panel {
      border: 1px solid var(--line);
      border-radius: 22px;
      background: var(--panel);
      backdrop-filter: blur(18px);
      padding: 20px;
      box-shadow: 0 20px 60px rgba(15, 23, 42, 0.08);
    }
    h2 { margin-top: 0; font-size: 1rem; text-transform: uppercase; letter-spacing: 0.08em; }
    .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-bottom: 14px; }
    input, button, select {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
    }
    input { min-width: 280px; background: rgba(255,255,255,0.92); }
    button {
      background: var(--accent);
      color: white;
      border: 0;
      cursor: pointer;
    }
    button.secondary { background: #334155; }
    .list { display: grid; gap: 10px; }
    .mini-list { display: grid; gap: 8px; margin-top: 10px; }
    .card {
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.72);
      border-radius: 16px;
      padding: 14px;
    }
    .mono { font-family: "SFMono-Regular", "Menlo", monospace; font-size: 0.92rem; }
    .status-succeeded { color: var(--accent); }
    .status-failed { color: var(--danger); }
    pre {
      margin: 0;
      padding: 16px;
      border-radius: 16px;
      background: #0f172a;
      color: #dbeafe;
      overflow: auto;
      min-height: 280px;
    }
    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
      input { min-width: 0; width: 100%; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>Workflow Control Plane</h1>
      <p>Inspect registered workflows, trigger runs, and render stored execution traces from a single static control surface.</p>
    </section>
    <section class="grid">
      <div class="panel">
        <h2>Runs</h2>
        <div class="row">
          <input id="token" placeholder="Bearer token (optional)">
          <button id="refresh">Refresh</button>
        </div>
        <div class="row">
          <select id="workflow"></select>
          <button id="run">Run Workflow</button>
        </div>
        <div class="row">
          <button id="enqueue" class="secondary">Enqueue Workflow</button>
          <button id="process" class="secondary">Process Next Job</button>
        </div>
        <div id="runs" class="list"></div>
        <h2 style="margin-top:18px;">Queue</h2>
        <div id="queue" class="mini-list"></div>
      </div>
      <div class="panel">
        <h2>Trace</h2>
        <pre id="trace" class="mono">Select a run to view its trace.</pre>
        <h2 style="margin-top:18px;">Live</h2>
        <pre id="events" class="mono">Waiting for events.</pre>
      </div>
    </section>
  </div>
  <script>
    const tokenInput = document.getElementById('token');
    const workflowSelect = document.getElementById('workflow');
    const runsContainer = document.getElementById('runs');
    const traceBox = document.getElementById('trace');
    const eventsBox = document.getElementById('events');
    const queueBox = document.getElementById('queue');

    function headers() {
      const token = tokenInput.value.trim();
      return token ? { Authorization: `Bearer ${token}` } : {};
    }

    function tokenQuery() {
      const token = tokenInput.value.trim();
      return token ? `?access_token=${encodeURIComponent(token)}` : '';
    }

    async function loadWorkflows() {
      const response = await fetch(`/workflows${tokenQuery()}`, { headers: headers() });
      const payload = await response.json();
      workflowSelect.innerHTML = '';
      for (const name of payload.workflows || []) {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        workflowSelect.appendChild(option);
      }
    }

    async function loadRuns() {
      const response = await fetch(`/runs?limit=15${tokenQuery() ? `&access_token=${encodeURIComponent(tokenInput.value.trim())}` : ''}`, { headers: headers() });
      const payload = await response.json();
      runsContainer.innerHTML = '';
      for (const run of payload.runs || []) {
        const card = document.createElement('button');
        card.className = 'card';
        card.style.textAlign = 'left';
        card.innerHTML = `
          <div class="mono">${run.run_id}</div>
          <div><strong>${run.workflow_name}</strong></div>
          <div class="status-${run.status}">${run.status}</div>
          <div>${run.total_wall_time_seconds.toFixed(4)}s wall / ${run.critical_path_seconds.toFixed(4)}s critical</div>
        `;
        card.onclick = () => loadTrace(run.run_id);
        runsContainer.appendChild(card);
      }
    }

    async function loadQueue() {
      const suffix = tokenQuery() ? `?limit=12&access_token=${encodeURIComponent(tokenInput.value.trim())}` : '?limit=12';
      const response = await fetch(`/queue${suffix}`, { headers: headers() });
      const payload = await response.json();
      queueBox.innerHTML = '';
      for (const job of payload.jobs || []) {
        const card = document.createElement('div');
        card.className = 'card';
        card.innerHTML = `
          <div class="mono">${job.job_id}</div>
          <div><strong>${job.workflow_name}</strong></div>
          <div>status=${job.status} attempts=${job.attempt_count}/${job.max_attempts}</div>
          <div>${job.last_error || 'no error'}</div>
        `;
        queueBox.appendChild(card);
      }
    }

    async function loadTrace(runId) {
      const response = await fetch(`/traces/${runId}${tokenQuery()}`, { headers: headers() });
      const payload = await response.json();
      traceBox.textContent = JSON.stringify(payload, null, 2);
    }

    async function runWorkflow() {
      const name = workflowSelect.value;
      await fetch(`/runs/${name}${tokenQuery()}`, { method: 'POST', headers: headers() });
      await loadRuns();
      await loadQueue();
    }

    async function enqueueWorkflow() {
      const name = workflowSelect.value;
      await fetch(`/queue/${name}${tokenQuery()}`, { method: 'POST', headers: headers() });
      await loadQueue();
    }

    async function processWorkflow() {
      await fetch(`/queue/process${tokenQuery()}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...headers() },
        body: JSON.stringify({ worker_id: 'ui-worker', lease_seconds: 15 })
      });
      await loadRuns();
      await loadQueue();
    }

    function connectEvents() {
      const source = new EventSource(`/events${tokenQuery()}`);
      source.onmessage = (event) => {
        const lines = (eventsBox.textContent || '').split('\n').slice(-20);
        lines.push(event.data);
        eventsBox.textContent = lines.join('\n');
        loadQueue();
        loadRuns();
      };
    }

    document.getElementById('refresh').onclick = async () => { await loadWorkflows(); await loadRuns(); await loadQueue(); };
    document.getElementById('run').onclick = runWorkflow;
    document.getElementById('enqueue').onclick = enqueueWorkflow;
    document.getElementById('process').onclick = processWorkflow;
    loadWorkflows().then(loadRuns).then(loadQueue);
    connectEvents();
  </script>
</body>
</html>"""
