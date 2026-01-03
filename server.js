import Fastify from "fastify";
import { request } from "undici";
import pRetry from "p-retry";
import crypto from "crypto";

const app = Fastify({ logger: true });
const SAM_URL = "https://api.sam.gov/opportunities/v2/search";

function stableHash(obj) {
  const json = JSON.stringify(obj, Object.keys(obj).sort());
  const h = crypto.createHash("sha256").update(json).digest("hex");
  return `sha256:${h}`;
}

function normalize(item) {
  const noticeId = item.noticeId || item.noticeID || item.id;
  const out = {
    noticeId,
    title: item.title ?? "",
    solicitationNumber: item.solicitationNumber ?? "",
    type: item.type ?? item.baseType ?? "",
    postedDate: item.postedDate ?? "",
    responseDeadline: item.responseDeadLine ?? item.responseDeadline ?? "",
    setAsideCode: item.typeOfSetAside ?? item.setAsideCode ?? "",
    naicsCode: item.naicsCode ?? "",
    classificationCode: item.classificationCode ?? "",
    uiLink: item.uiLink ?? "",
    active: true,
    raw: item
  };

  out.contentHash = stableHash({
    title: out.title,
    solicitationNumber: out.solicitationNumber,
    type: out.type,
    postedDate: out.postedDate,
    responseDeadline: out.responseDeadline,
    setAsideCode: out.setAsideCode,
    naicsCode: out.naicsCode,
    classificationCode: out.classificationCode,
    uiLink: out.uiLink
  });

  return out;
}

async function samSearch({ apiKey, postedFrom, postedTo, setAside, limit, offset }) {
  const url = new URL(SAM_URL);
  url.searchParams.set("api_key", apiKey);
  url.searchParams.set("postedFrom", postedFrom);
  url.searchParams.set("postedTo", postedTo);
  url.searchParams.set("status", "active"); // only active
  if (setAside) url.searchParams.set("typeOfSetAside", setAside);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));

  const res = await request(url.toString(), { method: "GET" });
  const body = await res.body.json().catch(() => ({}));

  if (res.statusCode === 429 || res.statusCode >= 500) {
    throw new Error(`RETRYABLE ${res.statusCode}`);
  }
  if (res.statusCode >= 400) {
    throw new Error(`SAM failed ${res.statusCode}: ${JSON.stringify(body).slice(0, 500)}`);
  }
  return body;
}

return pRetry(() => samSearch(args), {
  retries: 10,
  factor: 2,
  minTimeout: 2000,
  maxTimeout: 30000,
  randomize: true
});


app.get("/health", async () => ({ ok: true }));

app.post("/fetch", async (req, reply) => {
  const apiKey = process.env.SAM_API_KEY;
  const authToken = process.env.FETCHER_AUTH_TOKEN;

  if (!apiKey) return reply.code(500).send({ error: "Missing SAM_API_KEY env var" });

  // Simple protection so random people can’t hit your fetcher
  if (authToken) {
    const headerToken = req.headers["x-auth-token"];
    if (headerToken !== authToken) return reply.code(401).send({ error: "Unauthorized" });
  }

  const { postedFrom, postedTo, setAsides = ["SBA"], limit = 1000 } = req.body || {};
  if (!postedFrom || !postedTo) return reply.code(400).send({ error: "postedFrom and postedTo required" });

  const items = [];
  let pages = 0;

  for (const setAside of setAsides) {
    let offset = 0;
    while (true) {
      const data = await samSearchWithRetry({ apiKey, postedFrom, postedTo, setAside, limit, offset });

      const results =
        data.opportunitiesData ||
        data.opportunities ||
        data.results ||
        [];

      const normalized = results.map(normalize).filter(x => x.noticeId);
      items.push(...normalized);
      pages += 1;

      if (results.length < limit) break;
      offset += limit;
    }
  }

  return { meta: { postedFrom, postedTo, setAsides, fetched: items.length, pages }, items };
});

const port = process.env.PORT || 3000;
app.listen({ port, host: "0.0.0.0" });
