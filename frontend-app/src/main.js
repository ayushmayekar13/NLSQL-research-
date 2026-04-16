import './style.css';

/* ── API CONFIG ── */
const API_BASE = 'http://localhost:8000/api';
let sessionId = null;

function escHtml(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function renderSQL(sql) {
  if (!sql) return '<span class="cmt">-- No SQL generated</span>';
  let x = escHtml(sql);
  const kw = [
    'SELECT','FROM','WHERE','JOIN','LEFT','RIGHT','INNER','OUTER','ON',
    'GROUP BY','ORDER BY','LIMIT','HAVING','WITH','AS','AND','OR','DISTINCT'
  ];
  kw.forEach(k => {
    const re = new RegExp('\\b' + k.replace(' ', '\\s+') + '\\b', 'gi');
    x = x.replace(re, (m) => '<span class="kw">' + m.toUpperCase() + '</span>');
  });
  return x;
}

async function apiPost(path, body) {
  const res = await fetch(API_BASE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {})
  });
  const txt = await res.text();
  let json = null;
  try { json = txt ? JSON.parse(txt) : null; } catch (_) {}
  if (!res.ok) {
    const msg = (json && (json.message || json.detail || json.error)) ? JSON.stringify(json) : (txt || ('HTTP ' + res.status));
    throw new Error(msg);
  }
  return json;
}

async function apiGet(path) {
  const res = await fetch(API_BASE + path, { method: 'GET' });
  const txt = await res.text();
  let json = null;
  try { json = txt ? JSON.parse(txt) : null; } catch (_) {}
  if (!res.ok) {
    const msg = (json && (json.message || json.detail || json.error)) ? JSON.stringify(json) : (txt || ('HTTP ' + res.status));
    throw new Error(msg);
  }
  return json;
}

function renderSchemaExplorer(schemaData) {
  const grid = document.getElementById('schema-grid');
  if (!grid) return;
  const tables = Object.keys(schemaData || {});
  if (tables.length === 0) {
    grid.innerHTML = '<div class="schema-card"><div class="schema-card-head"><div class="schema-tname">No schema</div><div class="schema-cnt">0 columns</div></div><div class="schema-cols-list"><div class="schema-col-row"><span class="schema-col-name">No data available</span><span class="schema-col-type">N/A</span></div></div></div>';
    return;
  }

  grid.innerHTML = tables.map(function(tableName){
    const info = schemaData[tableName] || {};
    const columns = Array.isArray(info.columns) ? info.columns : [];
    const pks = new Set(Array.isArray(info.primary_keys) ? info.primary_keys : []);
    const fkCols = new Set(Array.isArray(info.foreign_keys) ? info.foreign_keys.map(f => f.column) : []);
    const colsHtml = columns.map(function(c){
      const colName = c && c.column_name ? String(c.column_name) : '';
      const dataType = c && c.data_type ? String(c.data_type).toUpperCase() : 'UNKNOWN';
      const tag = pks.has(colName)
        ? '<span class="pk-tag">PK</span>'
        : (fkCols.has(colName) ? '<span class="fk-tag">FK</span>' : '');
      return '<div class="schema-col-row"><span class="schema-col-name">' + escHtml(colName) + '</span><div style="display:flex;gap:4px;align-items:center">' + tag + '<span class="schema-col-type">' + escHtml(dataType) + '</span></div></div>';
    }).join('');

    return '<div class="schema-card"><div class="schema-card-head"><div class="schema-tname">' + escHtml(tableName) + '</div><div class="schema-cnt">' + columns.length + ' columns</div></div><div class="schema-cols-list">' + colsHtml + '</div></div>';
  }).join('');
}

/* ── DB TYPE SELECT ── */
var selectedEngine = 'postgresql';
window.selectDB = function(el, engine, port) {
  document.querySelectorAll('.db-btn').forEach(b => {
    b.classList.remove('active');
    b.querySelectorAll('circle,path,rect,ellipse').forEach(s => s.setAttribute('stroke','#8b909e'));
  });
  el.classList.add('active');
  el.querySelectorAll('circle,path,rect,ellipse').forEach(s => s.setAttribute('stroke','#19c490'));
  selectedEngine = engine;
  document.getElementById('inp-port').value = port;
  if (engine === 'sqlite') {
    document.getElementById('inp-host').value = 'local file';
    document.getElementById('inp-dbname').value = 'database.db';
    document.getElementById('inp-user').value = '';
    document.getElementById('inp-pass').value = '';
  } else {
    document.getElementById('inp-host').value = 'localhost';
    document.getElementById('inp-dbname').value = 'ecommerce_db';
    document.getElementById('inp-user').value = 'root';
  }
};

/* ── TEST CONNECTION ── */
window.testConn = async function() {
  var s = document.getElementById('conn-status');
  var m = document.getElementById('conn-msg');
  s.classList.remove('err');
  m.textContent = 'Testing connection…';
  s.classList.add('show');

  const payload = {
    engine: selectedEngine,
    host: document.getElementById('inp-host').value,
    port: Number(document.getElementById('inp-port').value || 0) || null,
    database: document.getElementById('inp-dbname').value,
    username: document.getElementById('inp-user').value,
    password: document.getElementById('inp-pass').value,
  };

  try {
    const out = await apiPost('/connect', payload);
    if (out.ok) {
      s.classList.remove('err');
      m.textContent = 'Connected (Qdrant + PostgreSQL validated)';
    } else {
      s.classList.add('err');
      m.textContent = (out.message || 'Connection failed');
    }
  } catch (e) {
    s.classList.add('err');
    m.textContent = 'Connection failed — ' + (e && e.message ? e.message : 'unknown error');
  }
};

/* ── GO TO LOADING SCREEN ── */
window.goLoading = async function() {
  var dbname = document.getElementById('inp-dbname').value || 'database';
  document.getElementById('loading-sub').textContent = 'Connecting to ' + dbname + '…';
  show('s-loading');
  var steps = [
    {id:'pp1', msg:'Extracting schema…'},
    {id:'pp2', msg:'Generating embeddings…'},
    {id:'pp3', msg:'Building vector index…'},
    {id:'pp4', msg:'Finalising pipeline…'},
    {id:'pp5', msg:'Ready!'},
  ];
  var checkSVG = '<svg class="pp-icon" viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="6" stroke="#19c490" stroke-width="1.5"/><path d="M5 8l2 2 4-4" stroke="#19c490" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  var spinHTML = '<div class="spinner"></div>';

  const payload = {
    engine: selectedEngine,
    host: document.getElementById('inp-host').value,
    port: Number(document.getElementById('inp-port').value || 0) || null,
    database: document.getElementById('inp-dbname').value,
    username: document.getElementById('inp-user').value,
    password: document.getElementById('inp-pass').value,
  };
  let connectOk = false;
  let connectMessage = '';
  let schemaPayload = null;
  apiPost('/connect', payload)
    .then(async out => {
      connectOk = !!out.ok;
      connectMessage = out.message || '';
      if (connectOk) {
        try {
          schemaPayload = await apiGet('/schema');
        } catch (e) {
          schemaPayload = null;
        }
      }
    })
    .catch(err => { connectOk = false; connectMessage = (err && err.message) ? err.message : 'connect failed'; });

  function doStep(i) {
    if (i >= steps.length) {
      setTimeout(function(){
        if (!connectOk) {
          show('s-connect');
          var s = document.getElementById('conn-status');
          var m = document.getElementById('conn-msg');
          s.classList.add('show');
          s.classList.add('err');
          m.textContent = 'Connect failed — ' + connectMessage;
          return;
        }
        var eng = selectedEngine.charAt(0).toUpperCase() + selectedEngine.slice(1);
        document.getElementById('topbar-db').textContent = dbname + ' · ' + eng;
        if (schemaPayload && schemaPayload.ok) {
          renderSchemaExplorer(schemaPayload.schema_data || {});
          document.getElementById('schema-meta').textContent = dbname + ' · ' + eng + ' · ' + String(schemaPayload.table_count || 0) + ' tables';
        } else {
          document.getElementById('schema-meta').textContent = dbname + ' · ' + eng + ' · schema unavailable';
        }
        show('s-app');
      }, 500);
      return;
    }
    var el = document.getElementById(steps[i].id);
    el.classList.add('doing');
    el.innerHTML = spinHTML + '<span>' + el.querySelector('span').textContent + '</span>';
    document.getElementById('loading-sub').textContent = steps[i].msg;
    setTimeout(function(){
      el.classList.remove('doing');
      el.classList.add('done');
      el.innerHTML = checkSVG + '<span>' + el.querySelector('span').textContent + '</span>';
      if (i + 1 < steps.length) {
        var next = document.getElementById(steps[i+1].id);
        next.classList.add('doing');
      }
      doStep(i + 1);
    }, 700);
  }
  doStep(0);
};

/* ── SCREEN SWITCH ── */
function show(id) {
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  document.getElementById(id).classList.add('active');
}
window.disconnect = function() { show('s-connect'); };

/* ── TAB SWITCH ── */
window.switchTab = function(tab, btn) {
  document.querySelectorAll('.nav-pill').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
};

/* ── HISTORY DATA ── */
var historyData = [
  {
    query: "Welcome to QueryNL2SQL!",
    type: "SRD", note: "Ready to query the database",
    tables: [{name:"instructions", cols:"type a query below"}],
    sql: '<span class="kw">SELECT</span> <span class="val">*</span> <span class="kw">FROM</span> users <span class="kw">LIMIT</span> <span class="val">1</span>;',
    raw_sql: 'SELECT * FROM users LIMIT 1;',
    thead: ["status"],
    tbody: [["Type a natural language query below and hit Run query."]]
  }
];

let activeIdx = 0;

window.loadHistory = function(el, idx) {
  if (!historyData[idx]) return;
  activeIdx = idx;
  document.querySelectorAll('.hist-li').forEach(h => h.classList.remove('active'));
  if (el) el.classList.add('active');
  var d = historyData[idx];
  document.getElementById('q-text').textContent = d.query;
  var typeChip = document.getElementById('q-type-chip');
  typeChip.textContent = d.type === 'SRD' ? 'SRD — single-round query' : 'MRD — multi-round query';
  typeChip.className = 'chip ' + (d.type ? d.type.toLowerCase() : 'srd');
  document.getElementById('q-note').textContent = d.note;
  var tablesHTML = d.tables.map(function(t){
    return '<div class="tbl-chip"><div class="tbl-name">'+t.name+'</div><div class="tbl-cols">'+t.cols+'</div></div>';
  }).join('');
  document.getElementById('tables-matched').innerHTML = tablesHTML;
  document.getElementById('sql-display').innerHTML = d.sql;
  var thead = d.thead.map(function(h){ return '<th>'+h+'</th>'; }).join('');
  var tbody = d.tbody.map(function(r){
    return '<tr>'+r.map(function(c){ return '<td>'+c+'</td>'; }).join('')+'</tr>';
  }).join('');
  document.querySelector('.res-table thead tr').innerHTML = thead;
  document.getElementById('res-body').innerHTML = tbody;
};

function renderHistorySidebar() {
  const el = document.getElementById('hist-list');
  el.innerHTML = historyData.map(function(d, i){
    const tagClass = d.type === 'MRD' ? 'mrd' : (d.type==='ERROR' ? 'mrd':'srd'); 
    return '<div class="hist-li' + (i === activeIdx ? ' active' : '') + '" onclick="loadHistory(this, ' + i + ')">' +
      '<span class="htag ' + tagClass + '">' + escHtml((d.type||'').substring(0,3)) + '</span>' + 
      escHtml(d.query.substring(0,32) + (d.query.length > 32 ? '…' : '')) + '</div>';
  }).join('');
}

/* ── RUN QUERY ── */
window.runQuery = async function() {
  var input = document.getElementById('nl-input');
  var q = input.value.trim();
  if (!q) return;

  var newEntry = {
    query: q,
    type: "SRD", note: "Generating SQL...",
    tables: [{name: "...", cols: "matching tables via vector search"}],
    sql: '<span class="cmt">-- Generating SQL for: ' + escHtml(q) + '</span>',
    raw_sql: '',
    thead: ["status", "message"],
    tbody: [["executing", "SQL is being generated."]]
  };

  historyData.unshift(newEntry);
  activeIdx = 0;
  renderHistorySidebar();
  window.loadHistory(document.querySelector('.hist-li.active'), 0);

  try {
    const out = await apiPost('/query', { query: q, top_k: 5, session_id: sessionId });
    sessionId = out.session_id;

    const type = out.query_type || 'UNKNOWN';
    const warnings = Array.isArray(out.warnings) ? out.warnings : [];
    const baseNote = (type === 'MRD' ? 'Multi-round · combined with previous turns' : 'Standalone · no prior context needed');
    
    newEntry.type = type;
    newEntry.note = warnings.length ? (baseNote + ' · ' + warnings[0]) : baseNote;
    
    const schema = out.schema || {};
    const tbls = Object.keys(schema);
    if (tbls.length === 0) {
      newEntry.tables = [{name: "No match", cols: "No schema context found"}];
    } else {
      newEntry.tables = tbls.map(function(t){
        const cols = (schema[t] && schema[t].columns) ? schema[t].columns.map(c => c.column_name).slice(0,10).join(', ') : '';
        return {name: t, cols: cols || '...' };
      });
    }

    if (out.sql) {
      newEntry.sql = renderSQL(out.sql);
      newEntry.raw_sql = out.sql; 
    } else {
      const firstWarn = (warnings.length > 0) ? warnings[0] : 'No SQL generated (schema mismatch or model unavailable)';
      newEntry.sql = '<span class="cmt">-- ' + escHtml(firstWarn) + '</span>';
      newEntry.raw_sql = '';
    }

    newEntry.thead = ["status", "message", "session_id", "sql_valid"];
    newEntry.tbody = [
      ["ready", warnings.length ? warnings.join(' | ') : "SQL is ready. Click Execute to run.", out.session_id, String(!!out.sql_valid)]
    ];

  } catch (e) {
    newEntry.type = "ERROR";
    newEntry.note = "Backend error (see results panel)";
    newEntry.tables = [{name:"error", cols:"failed to fetch schema/sql"}];
    newEntry.sql = '<span class="cmt">-- Backend error: ' + escHtml(e && e.message ? e.message : 'unknown') + '</span>';
    newEntry.raw_sql = '';
    newEntry.thead = ['error'];
    newEntry.tbody = [['Backend error — ' + escHtml(e && e.message ? e.message : 'unknown')]];
  }

  renderHistorySidebar();
  window.loadHistory(document.querySelector('.hist-li.active'), 0);
  input.value = '';
};

/* ── EXECUTE SQL ── */
window.executeSQL = async function() {
  const d = historyData[activeIdx];
  if (!d || !d.raw_sql) {
      alert("No valid SQL found to execute!");
      return;
  }
  
  const payload = {
    engine: selectedEngine,
    host: document.getElementById('inp-host').value,
    port: Number(document.getElementById('inp-port').value || 0) || null,
    database: document.getElementById('inp-dbname').value,
    username: document.getElementById('inp-user').value,
    password: document.getElementById('inp-pass').value,
    sql: d.raw_sql
  };

  document.querySelector('.res-table thead tr').innerHTML = '<th>status</th>';
  document.getElementById('res-body').innerHTML = '<tr><td style="color:var(--teal)">Executing query on db...</td></tr>';

  try {
    const out = await apiPost('/execute', payload);
    if (!out.ok) {
       d.thead = ["error"];
       d.tbody = [[out.error]];
    } else if (out.columns && out.columns.length > 0) {
       d.thead = out.columns;
       d.tbody = out.rows.length ? out.rows : [["No rows returned."]];
    } else {
       d.thead = ["status"];
       d.tbody = [["Query executed successfully (no rows returned)."]];
    }
  } catch (e) {
       d.thead = ["error"];
       d.tbody = [[e.message]];
  }
  window.loadHistory(document.querySelector('.hist-li.active'), activeIdx);
};

window.handleKey = function(e) {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); window.runQuery(); }
};

/* ── COPY SQL ── */
window.copySQL = function() {
  var text = document.getElementById('sql-display').innerText;
  navigator.clipboard.writeText(text).catch(function(){});
  var btn = document.querySelector('.copy-btn');
  btn.textContent = 'copied!';
  setTimeout(function(){ btn.textContent = 'copy'; }, 1500);
};
