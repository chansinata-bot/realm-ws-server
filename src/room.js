// GameRoom: one Durable Object instance holds the whole game world's data
// (characters, inventories, mailboxes...) and relays realtime updates to
// every connected browser tab over WebSocket.
//
// Storage model: every piece of data lives under a "path" string, e.g.
//   rt/characters/<uid>
//   fs/mails/<mailId>
// Values are arbitrary JSON, stored with the Durable Object's built-in
// SQLite-backed storage (this.storage).

function uid() {
  return crypto.randomUUID().replace(/-/g, '').slice(0, 20);
}
function token() {
  return crypto.randomUUID().replace(/-/g, '');
}
function parentPath(path) {
  const i = path.lastIndexOf('/');
  return i < 0 ? '' : path.slice(0, i);
}
function lastSeg(path) {
  const i = path.lastIndexOf('/');
  return i < 0 ? path : path.slice(i + 1);
}

// --- Change these to control who is GM -------------------------------
// EVERYONE_GM = false  →  only uids listed in GM_UIDS are GM
const EVERYONE_GM = false;
const GM_UIDS = ['320ed424edc844e18fef', 'b264361fe2ad41598747'];
const GM_LOGIN_IDS = []; // not used yet — login system is still a stub
function isGM(session) {
  return EVERYONE_GM || (session.uid && GM_UIDS.includes(session.uid));
}
// -----------------------------------------------------------------------

export class GameRoom {
  constructor(state, env) {
    this.state = state;
    this.storage = state.storage;
    this.env = env;
    /** @type {Map<WebSocket, {uid:string, subs:Map<string,{path:string,kind:string}>, od:Array<{path:string}>}>} */
    this.sessions = new Map();
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('expected websocket', { status: 426 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    const session = { uid: null, subs: new Map(), od: [] };
    this.sessions.set(server, session);

    server.addEventListener('message', ev => {
      this.onMessage(server, session, ev.data).catch(err => {
        try { server.send(JSON.stringify({ err: 'unavailable', detail: String(err && err.message || err) })); } catch (e) {}
      });
    });
    server.addEventListener('close', () => this.onClose(server, session));
    server.addEventListener('error', () => this.onClose(server, session));

    return new Response(null, { status: 101, webSocket: client });
  }

  async onClose(ws, session) {
    for (const act of session.od.splice(0)) {
      await this.applySet(act.path, null);
    }
    this.sessions.delete(ws);
  }

  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch (e) {} }

  async onMessage(ws, session, raw) {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    const reply = (body) => { if (m.i) this.send(ws, Object.assign({ i: m.i }, body)); };

    switch (m.op) {
      case 'hello': {
        let acc = null;
        if (m.uid && m.tok) {
          const row = await this.storage.get('acct/' + m.uid);
          if (row && row.tok === m.tok) acc = row;
        }
        if (!acc) {
          const id = uid(), tok = token();
          acc = { uid: id, tok, loginId: null, email: null, created: Date.now() };
          await this.storage.put('acct/' + id, acc);
        }
        session.uid = acc.uid;
        const gm = isGM(session);
        reply({ uid: acc.uid, tok: acc.tok, id: acc.loginId, email: acc.email, time: Date.now(), gid: 'main', gm });
        break;
      }
      case 'ping': reply({}); break;
      case 'out': reply({}); break;

      case 'get': {
        const val = await this.storage.get(m.path);
        reply({ val: val === undefined ? null : val });
        break;
      }
      case 'set': {
        await this.applySet(m.path, m.val === undefined ? null : m.val);
        reply({});
        break;
      }
      case 'upd': {
        const cur = (await this.storage.get(m.path)) || {};
        const next = Object.assign({}, cur, m.vals);
        await this.applySet(m.path, next);
        reply({});
        break;
      }
      case 'rm': {
        await this.applySet(m.path, null);
        reply({});
        break;
      }
      case 'cas': {
        const cur = await this.storage.get(m.path);
        const curJson = JSON.stringify(cur === undefined ? null : cur);
        if (curJson !== m.expect) { reply({ conflict: true }); break; }
        await this.applySet(m.path, m.val === undefined ? null : m.val);
        reply({});
        break;
      }

      case 'sub': {
        session.subs.set(m.sid, { path: m.path, kind: m.kind });
        if (m.kind === 'v') {
          const val = await this.storage.get(m.path);
          this.send(ws, { sid: m.sid, ev: 'i', val: val === undefined ? null : val });
        } else {
          const kids = await this.listChildren(m.path);
          this.send(ws, { sid: m.sid, ev: 'i', val: kids });
        }
        reply({});
        break;
      }
      case 'unsub': {
        session.subs.delete(m.sid);
        reply({});
        break;
      }
      case 'od': session.od.push({ path: m.path }); reply({}); break;
      case 'odc': session.od = session.od.filter(a => a.path !== m.path); reply({}); break;

      case 'q': {
        const prefix = m.path + '/';
        const list = await this.storage.list({ prefix });
        let rows = [...list.entries()].map(([k, v]) => [k.slice(prefix.length), v]);
        for (const w of (m.where || [])) {
          rows = rows.filter(r => r[1] && cmp(r[1][w[0]], w[1], w[2]));
        }
        if (m.ob) {
          const [f, d] = m.ob, dir = d === 'desc' ? -1 : 1;
          rows.sort((a, b) => (a[1][f] < b[1][f] ? -1 : a[1][f] > b[1][f] ? 1 : 0) * dir);
        }
        if (m.limit) rows = rows.slice(0, m.limit);
        reply({ docs: rows });
        break;
      }

      // ---- GM tools (see gm_ext.js / game.js gmCheck()) ------------------
      case 'gmq': {
        reply({ gm: isGM(session) });
        break;
      }
      case 'gmgift': {
        if (!isGM(session)) { reply({ err: 'permission-denied' }); break; }
        const targets = [];
        if (m.to) targets.push(m.to);
        else for (const s of this.sessions.values()) if (s.uid) targets.push(s.uid);
        let sentCount = 0;
        for (const t of targets) {
          const box = (await this.storage.get('mbox/' + t)) || [];
          box.push({ items: m.items || [], gold: m.gold || 0, msg: m.msg || '', gm: 1, ts: Date.now() });
          await this.storage.put('mbox/' + t, box);
          sentCount++;
        }
        if (m.to) reply({ sent: sentCount, to: m.to, online: targets.length > 0 });
        else reply({ sent: sentCount });
        break;
      }
      case 'gmpeek': {
        if (!isGM(session)) { reply({ err: 'permission-denied' }); break; }
        // Minimal stub: without the character-lookup-by-name index this
        // can only report "not found". Extend once character data is saved.
        reply({ ok: false, err: 'not-found' });
        break;
      }
      case 'gmwarp': {
        // GM-only: warp a character (or all online characters) to any map.
        //
        // Request fields:
        //   m.map   {string}  required — target map id, e.g. "town", "dungeon_1"
        //   m.x     {number}  optional — spawn X on the target map (default 0)
        //   m.y     {number}  optional — spawn Y on the target map (default 0)
        //   m.to    {string}  optional — uid of specific player to warp;
        //                               omit to warp every online character
        //
        // What it does:
        //   1. Writes the new map/x/y into rt/characters/<uid> in storage
        //      so the data persists even if the player reconnects.
        //   2. Pushes a "warp" event directly to the target player's
        //      WebSocket so the client can react instantly without polling.
        //
        // Reply: { warped: number }  — how many characters were moved.

        if (!isGM(session)) { reply({ err: 'permission-denied' }); break; }
        if (!m.map) { reply({ err: 'invalid-argument', detail: 'map is required' }); break; }

        const targetMap = String(m.map);
        const targetX   = typeof m.x === 'number' ? m.x : 0;
        const targetY   = typeof m.y === 'number' ? m.y : 0;

        // Build list of (uid, ws) pairs to warp
        const targets = [];
        if (m.to) {
          // warp a specific uid — they don't have to be online
          targets.push({ uid: m.to, ws: null });
          for (const [ws, s] of this.sessions) {
            if (s.uid === m.to) { targets[0].ws = ws; break; }
          }
        } else {
          // warp every online character
          for (const [ws, s] of this.sessions) {
            if (s.uid) targets.push({ uid: s.uid, ws });
          }
        }

        let warped = 0;
        for (const t of targets) {
          const charPath = 'rt/characters/' + t.uid;
          const cur = (await this.storage.get(charPath)) || {};
          const next = Object.assign({}, cur, { map: targetMap, x: targetX, y: targetY });
          await this.applySet(charPath, next);   // persist + notify subscribers

          // Also push an explicit warp event so the client can
          // teleport the sprite immediately without waiting for a sub update.
          if (t.ws) {
            this.send(t.ws, { ev: 'warp', map: targetMap, x: targetX, y: targetY, by: 'gm' });
          }
          warped++;
        }

        reply({ warped, map: targetMap, x: targetX, y: targetY });
        break;
      }
      case 'resetall': {
        if (!isGM(session)) { reply({ err: 'permission-denied' }); break; }
        if (m.confirm !== 'RESET') { reply({ ok: false, err: 'invalid-argument' }); break; }
        // NOTE: this is a stub. Wire this up to your own backup + wipe
        // logic once you decide exactly what "reset everything" should
        // back up and delete in this storage model.
        reply({ ok: false, err: 'unavailable' });
        break;
      }
      case 'epoch': {
        const e = (await this.storage.get('meta/epoch')) || 0;
        reply({ e });
        break;
      }
      case 'gift': {
        // Welcome-gift claim stub: tracks one claim per uid.
        const key = 'gift/' + (session.uid || '');
        const already = await this.storage.get(key);
        if (already) { reply({ ok: true, got: false }); break; }
        await this.storage.put(key, { ts: Date.now() });
        reply({ ok: true, got: true });
        break;
      }
      case 'mbox': {
        const box = (await this.storage.get('mbox/' + m.cid)) || [];
        reply({ box });
        break;
      }

      // ---- Account ops: minimal, anonymous-first ------------------------
      case 'code': reply({ code: (session.uid || '').slice(0, 6) || 'NOCODE' }); break;
      case 'usecode': reply({ err: 'not-found' }); break;
      case 'google': reply({ err: 'unavailable' }); break;
      case 'register': reply({ err: 'unavailable' }); break;
      case 'login': reply({ err: 'unavailable' }); break;
      case 'recover': reply({ err: 'unavailable' }); break;
      case 'rcnew': reply({ err: 'unavailable' }); break;

      default:
        reply({ err: 'unavailable' });
    }
  }

  async listChildren(path) {
    const prefix = path + '/';
    const list = await this.storage.list({ prefix });
    const out = {};
    for (const [k, v] of list.entries()) {
      const rest = k.slice(prefix.length);
      if (rest.includes('/')) continue; // only direct children
      out[rest] = v;
    }
    return out;
  }

  async applySet(path, val) {
    if (val === null) await this.storage.delete(path);
    else await this.storage.put(path, val);

    // notify exact-path "value" subscribers
    for (const session of this.sessions.values()) {
      for (const [sid, sub] of session.subs) {
        if (sub.kind === 'v' && sub.path === path) {
          this.notify(session, sid, { ev: 'v', val });
        }
      }
    }
    // notify parent "children" subscribers
    const parent = parentPath(path), key = lastSeg(path);
    for (const session of this.sessions.values()) {
      for (const [sid, sub] of session.subs) {
        if (sub.kind === 'c' && sub.path === parent) {
          this.notify(session, sid, { ev: 'c', key, val });
        }
      }
    }
  }

  notify(session, sid, body) {
    for (const [ws, s] of this.sessions) {
      if (s === session) { this.send(ws, Object.assign({ sid }, body)); return; }
    }
  }
}

function cmp(a, op, b) {
  switch (op) {
    case '==': return a === b;
    case '!=': return a !== b;
    case '<': return a < b;
    case '<=': return a <= b;
    case '>': return a > b;
    case '>=': return a >= b;
    default: return false;
  }
}
