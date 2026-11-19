// Vercel serverless function: shared database for Solo Leveling KK (ranks, friend requests, posts, videos...).
// Storage: Upstash Redis from the Vercel Marketplace. The env vars below are added automatically when you connect it.
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

const COLS = ['hunters', 'posts', 'reactions', 'challenges', 'reqs', 'msgs', 'videos'];
const okCol = (c) => COLS.indexOf(c) > -1;
const okId = (s) => typeof s === 'string' && /^[A-Za-z0-9_\-\/]{1,120}$/.test(s);
const okVid = (s) => typeof s === 'string' && /^fb[a-z0-9]{6,40}$/.test(s);
const MAXDOC = 700000; // characters in one stored document / video part
const MAXRESP = 4200000; // Vercel responses are limited to about 4.5 MB

async function redis(cmds) {
  const r = await fetch(URL_ + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error('redis ' + r.status);
  const out = await r.json();
  return out.map((x) => {
    if (x && x.error) throw new Error(x.error);
    return x.result;
  });
}
const one = async (cmd) => (await redis([cmd]))[0];

function send(res, code, body, cache) {
  res.statusCode = code;
  res.setHeader('Cache-Control', cache || 'no-store');
  if (typeof body === 'string') {
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.end(body);
  } else {
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.end(JSON.stringify(body));
  }
}
const toObj = (flat) => {
  const o = {};
  for (let i = 0; i < (flat || []).length; i += 2) o[flat[i]] = flat[i + 1];
  return o;
};

module.exports = async (req, res) => {
  try {
    if (!URL_ || !TOKEN) return send(res, 500, { error: 'Storage not connected' });
    const q = req.query || {};

    if (req.method === 'GET') {
      const op = q.op;
      if (op === 'ping') {
        const p = await one(['PING']);
        return send(res, 200, { ok: 1, db: p === 'PONG' });
      }
      if (op === 'ver') {
        // tiny and cached for 3 seconds at the CDN, so many users cost almost nothing
        const v = toObj(await one(['HGETALL', 'ver']));
        return send(res, 200, v, 'public, max-age=0, s-maxage=3, stale-while-revalidate=5');
      }
      if (op === 'col') {
        if (!okCol(q.c)) return send(res, 400, { error: 'bad collection' });
        const d = toObj(await one(['HGETALL', 'c:' + q.c]));
        let size = 0;
        const keys = Object.keys(d);
        keys.forEach((k) => (size += (d[k] || '').length + k.length + 6));
        if (size > MAXRESP) {
          // too much data for one response: leave out the biggest documents
          keys.sort((a, b) => d[b].length - d[a].length);
          for (const k of keys) {
            if (size <= MAXRESP) break;
            size -= d[k].length;
            delete d[k];
          }
        }
        const cache = q.v ? 'public, max-age=3600, s-maxage=31536000, immutable' : 'no-store';
        return send(res, 200, { d: d }, cache);
      }
      if (op === 'get') {
        const c = q.c === 'priv' ? 'priv' : q.c;
        if (!(c === 'priv' || okCol(c)) || !okId(q.i)) return send(res, 400, { error: 'bad request' });
        const j = await one(['HGET', 'c:' + c, q.i]);
        return send(res, 200, { j: j || null });
      }
      if (op === 'vm') {
        if (!okVid(q.id)) return send(res, 400, { error: 'bad id' });
        const m = await one(['GET', 'vm:' + q.id]);
        if (!m) return send(res, 404, { error: 'missing' });
        return send(res, 200, m, 'public, max-age=3600, s-maxage=31536000, immutable');
      }
      if (op === 'vc') {
        const n = parseInt(q.n, 10);
        if (!okVid(q.id) || !(n >= 0 && n < 100)) return send(res, 400, { error: 'bad request' });
        const d = await one(['GET', 'vc:' + q.id + ':' + n]);
        if (!d) return send(res, 404, { error: 'missing' });
        return send(res, 200, d, 'public, max-age=3600, s-maxage=31536000, immutable');
      }
      return send(res, 400, { error: 'bad op' });
    }

    if (req.method === 'POST') {
      let b = req.body;
      if (typeof b === 'string') b = JSON.parse(b || '{}');
      b = b || {};
      if (b.op === 'set') {
        const c = b.c;
        if (!(c === 'priv' || okCol(c)) || !okId(b.i) || typeof b.j !== 'string' || b.j.length > MAXDOC)
          return send(res, 400, { error: 'bad request' });
        const cmds = [['HSET', 'c:' + c, b.i, b.j]];
        if (c !== 'priv') cmds.push(['HINCRBY', 'ver', c, 1]);
        const r = await redis(cmds);
        return send(res, 200, { ok: 1, v: c === 'priv' ? 0 : r[1] });
      }
      if (b.op === 'del') {
        const c = b.c;
        if (!(c === 'priv' || okCol(c)) || !okId(b.i)) return send(res, 400, { error: 'bad request' });
        const cmds = [['HDEL', 'c:' + c, b.i]];
        if (c !== 'priv') cmds.push(['HINCRBY', 'ver', c, 1]);
        await redis(cmds);
        return send(res, 200, { ok: 1 });
      }
      if (b.op === 'vput') {
        const n = parseInt(b.n, 10);
        if (!okVid(b.id) || !(n >= 0 && n < 100) || typeof b.d !== 'string' || b.d.length > MAXDOC || !/^[A-Za-z0-9+\/=]*$/.test(b.d))
          return send(res, 400, { error: 'bad request' });
        await one(['SET', 'vc:' + b.id + ':' + n, b.d]);
        return send(res, 200, { ok: 1 });
      }
      if (b.op === 'vmeta') {
        const n = parseInt(b.n, 10);
        if (!okVid(b.id) || !(n >= 1 && n < 100)) return send(res, 400, { error: 'bad request' });
        const t = /^video\/[a-z0-9.+-]{1,30}$/i.test(b.t || '') ? b.t : 'video/mp4';
        await one(['SET', 'vm:' + b.id, JSON.stringify({ t: t, n: n })]);
        return send(res, 200, { ok: 1 });
      }
      return send(res, 400, { error: 'bad op' });
    }
    return send(res, 405, { error: 'method' });
  } catch (e) {
    return send(res, 500, { error: 'server error' });
  }
};
