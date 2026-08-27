"""Build azure/app/index.html from ../availability_app.html.

The artifact version persists by republishing itself on claude.ai; the Azure
version persists through the /api/state Function (blob storage + ETag
optimistic concurrency), with background refresh so phones see each other's
saves. Everything else - UI, generator, flows - is byte-identical, so keep
editing availability_app.html and re-run this script.
"""

import re
from pathlib import Path

HERE = Path(__file__).parent
src = (HERE.parent / "availability_app.html").read_text()

# 1) three-mode badge (server / shared artifact / local)
old_setmode = """  function setMode(m) {
    mode = m;
    const b = document.getElementById('modebadge');
    if (m === 'shared') { b.textContent = 'Shared board'; b.classList.add('shared'); }
    else { b.textContent = 'This browser only'; b.classList.remove('shared'); }
  }"""
new_setmode = """  function setMode(m) {
    mode = m;
    const b = document.getElementById('modebadge');
    if (m === 'server') { b.textContent = 'Shared board (Azure)'; b.classList.add('shared'); }
    else if (m === 'shared') { b.textContent = 'Shared board'; b.classList.add('shared'); }
    else { b.textContent = 'This browser only'; b.classList.remove('shared'); }
  }
  let serverEtag = null;
  async function pullServer(force) {
    const res = await fetch('/api/state', { cache: 'no-store' });
    if (!res.ok) throw new Error('state api ' + res.status);
    const body = await res.json();
    if (body.state && (force || body.etag !== serverEtag)) {
      serverEtag = body.etag;
      STATE = body.state;
      if (me && me !== 'admin' && !gp(me)) me = null;
      if (!document.getElementById('dlg').open) renderAll();
    } else if (body.etag) serverEtag = body.etag;
    return body;
  }
  async function pushServer() {
    const res = await fetch('/api/state', {
      method: 'PUT', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ state: STATE, etag: serverEtag })
    });
    if (res.status === 409) { await pullServer(true); toast('Someone else saved first — reloaded the latest board.'); return 'conflict'; }
    if (!res.ok) throw new Error('save failed ' + res.status);
    serverEtag = (await res.json()).etag;
    return 'ok';
  }"""
assert old_setmode in src
src = src.replace(old_setmode, new_setmode)

# 2) persist(): server first, artifact second, browser-local last
m = re.search(r"  async function persist\(\) \{[\s\S]*?\n  \}\n(?=  function mergeLocal)", src)
assert m, "persist() block not found"
new_persist = """  async function persist() {
    if (mode === 'server') {
      try {
        const r = await pushServer();
        return r === 'conflict'; // conflict path already re-rendered
      } catch (e) {
        lsSave(STATE);
        toast('Could not reach the board service — change kept in this browser; it will not be visible to others until saved again.');
        return false;
      }
    }
    if (artifactNS) {
      try {
        const res = await fetch(location.href, { cache: 'no-store' });
        const srcText = await res.text();
        const json = JSON.stringify(STATE).replace(/<\\//g, '<\\\\/');
        const next = srcText.replace(/(<script type="application\\/json" id="app-state">)[\\s\\S]*?(<\\/script>)/, (m0, a, b) => a + json + b);
        await artifactNS.publish(next);
        return true;
      } catch (e) {
        if (String(e && (e.code || e.name || '')).includes('conflict')) { toast('Someone else saved first — reloading.'); return true; }
        artifactNS = null; setMode('local');
        toast('Shared save unavailable — keeping changes in this browser.');
      }
    }
    lsSave(STATE);
    return false;
  }
"""
src = src[:m.start()] + new_persist + src[m.end():]

# 3) boot: probe the Azure API before the claude artifact runtime
m = re.search(r"  \(async \(\) => \{[\s\S]*?\}\)\(\);\n\}\)\(\);", src)
assert m, "boot block not found"
new_boot = """  (async () => {
    try {
      const res = await fetch('/api/state', { cache: 'no-store' });
      if (res.ok) {
        const body = await res.json();
        if (body.state) { STATE = body.state; serverEtag = body.etag; }
        else {
          try { serverEtag = null; await pushServer(); } catch (e) {}
        }
        setMode('server');
        if (!STATE.published['2026-10']) STATE.published['2026-10'] = { grid: generate('2026-10'), at: 'sample' };
        renderAll();
        setInterval(() => { pullServer().catch(() => {}); }, 30000);
        window.addEventListener('focus', () => pullServer().catch(() => {}));
        return;
      }
    } catch (e) { /* no backend - fall through */ }
    try {
      if (window.claude && typeof window.claude.use === 'function') artifactNS = await window.claude.use('artifact');
    } catch (e) { artifactNS = null; }
    if (artifactNS) setMode('shared');
    else { setMode('local'); mergeLocal(); if (!STATE.published['2026-10']) STATE.published['2026-10'] = { grid: generate('2026-10'), at: 'sample' }; renderAll(); }
  })();
})();"""
src = src[:m.start()] + new_boot + src[m.end():]

# 4) wrap as a complete standalone document
head_end = src.index("</style>") + len("</style>")
html = ('<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        + src[:head_end] + "\n</head>\n<body>" + src[head_end:] + "\n</body>\n</html>\n")

(HERE / "app" / "index.html").write_text(html)
print(f"azure/app/index.html written ({len(html)/1024:.0f} KB)")
