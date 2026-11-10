// Vercel serverless function: global rank list for everyone who opens the site.
// Storage: Upstash Redis (Vercel Marketplace -> Upstash Redis). Env vars are added automatically:
//   KV_REST_API_URL + KV_REST_API_TOKEN   (or UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN)
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const HASH = 'hunters';

async function redis(cmd) {
  const r = await fetch(URL_, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd),
  });
  if (!r.ok) throw new Error('redis ' + r.status);
  return (await r.json()).result;
}

const clean = (s) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 24);
const int = (v, min, max) => Math.max(min, Math.min(max, Math.floor(+v) || 0));

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!URL_ || !TOKEN) return res.status(500).json({ error: 'Storage not connected' });
  try {
    if (req.method === 'POST') {
      const b = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : req.body || {};
      const id = String(b.id || '');
      if (!/^[a-z0-9]{16,40}$/.test(id)) return res.status(400).json({ error: 'bad id' });
      const n = clean(b.n);
      if (!n) return res.status(400).json({ error: 'bad name' });
      const old = await redis(['HGET', HASH, id]);
      let j = Date.now();
      try { if (old) j = JSON.parse(old).j || j; } catch (e) {}
      const rec = {
        n,
        l: int(b.l, 1, 100),
        x: int(b.x, 0, 1e9),
        s: int(b.s, 0, 99999),
        j,
        at: Date.now(),
      };
      await redis(['HSET', HASH, id, JSON.stringify(rec)]);
      return res.status(200).json({ ok: 1 });
    }
    if (req.method === 'GET') {
      const flat = (await redis(['HGETALL', HASH])) || [];
      const out = {};
      for (let i = 0; i < flat.length; i += 2) {
        try { out[flat[i]] = JSON.parse(flat[i + 1]); } catch (e) {}
      }
      return res.status(200).json(out);
    }
    return res.status(405).json({ error: 'method' });
  } catch (e) {
    return res.status(500).json({ error: 'server' });
  }
};
