// server.js
// Microservicio AEMET – caché por zona con refresco vía cron (admin) y lectura pública sin llamadas a AEMET
// --------------------------------------------------------------------------------------------------------
// Objetivo:
//   - Separar la CAPTURA desde AEMET (vía /admin/*, la invoca el cron de Render) de la CONSULTA pública (/avisos),
//     que solo lee de caché y NUNCA habla con AEMET.
//   - Acumular todos los avisos de una misma zona presentes en el TAR/XML del “ultimoelaborado” (sin sobrescribir).
//   - Deduplicar por (file + header.identifier) para evitar entradas repetidas.
// --------------------------------------------------------------------------------------------------------

import express from 'express';
import { fetch } from 'undici';
import * as zlib from 'zlib';
import tar from 'tar-stream';
import crypto from 'crypto';
import { XMLParser } from 'fast-xml-parser';

const app = express();
const PORT = process.env.PORT || 3000;
const UA = 'MT-Neo-Avisos-Zona/2.1';
const AEMET_API_KEY = process.env.AEMET_API_KEY || '';
const CRON_TOKEN = process.env.RENDER_CRON_TOKEN || '';
const CACHE_TTL_SECONDS = parseInt(process.env.CACHE_TTL_SECONDS || '1200', 10); // 20 min por defecto
const AREAS = (process.env.AREAS || '').split(',').map(s => s.trim()).filter(Boolean); // p.ej. "61,62,63,..."

app.use(express.json({ limit: '4mb' }));

// ========================= CACHÉ EN MEMORIA (por zona) =========================
//
// Estructura de cada entrada:
//   {
//     payload: { query, metadatos, ficheros[], avisos[] },
//     fetchedAt: <ms epoch>,
//     stale: boolean
//   }
// Consideramos expirada si la edad > CACHE_TTL_SECONDS.
//
const cacheZona = new Map(); // key: '614101' -> entry

function nowMs() { return Date.now(); }
function isExpired(entry) {
  if (!entry) return true;
  const ageSec = (nowMs() - entry.fetchedAt) / 1000;
  return ageSec > CACHE_TTL_SECONDS;
}

// ========================= UTILIDADES BÁSICAS =========================

function assertZona(z) {
  if (!/^\d{6}$/.test(z || '')) {
    const e = new Error('Parámetro "zona" inválido. Debe ser 6 dígitos (p.ej. 614101).');
    e.status = 400;
    throw e;
  }
}

function isGzip(buf) {
  return buf.length >= 2 && buf[0] === 0x1f && buf[1] === 0x8b;
}

async function fetchWithTimeout(url, options = {}, ms = 10000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try {
    return await fetch(url, { ...options, signal: ac.signal });
  } finally {
    clearTimeout(t);
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function tryFetchJSON(url, headers = {}, { retries = 2, timeoutMs = 10000 } = {}) {
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try {
      const r = await fetchWithTimeout(url, { headers: { accept: 'application/json', 'user-agent': UA, ...headers } }, timeoutMs);
      if (!r.ok) throw new Error(`HTTP ${r.status} en ${url}`);
      return await r.json();
    } catch (e) {
      lastErr = e;
      if (i < retries) {
        const backoff = Math.min(1000 * 2 ** i, 5000) + Math.random() * 250;
        await sleep(backoff);
      }
    }
  }
  throw lastErr;
}

async function tryFetchBuffer(url, headers = {}, { retries = 2, timeoutMs = 15000 } = {}) {
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try {
      const r = await fetchWithTimeout(url, { headers: { 'user-agent': UA, ...headers } }, timeoutMs);
      if (!r.ok) throw new Error(`HTTP ${r.status} al descargar datos (TAR/XML)`);
      const ab = await r.arrayBuffer();
      return Buffer.from(ab);
    } catch (e) {
      lastErr = e;
      if (i < retries) {
        const backoff = Math.min(1000 * 2 ** i, 5000) + Math.random() * 250;
        await sleep(backoff);
      }
    }
  }
  throw lastErr;
}

async function fetchJSONSmart(url, headers = {}) {
  // Lectura robusta por si los metadatos vienen con codificación rara
  const r = await fetchWithTimeout(url, { headers: { accept: 'application/json,*/*;q=0.8', 'user-agent': UA, ...headers } }, 10000);
  if (!r.ok) throw new Error(`HTTP ${r.status} en ${url}`);

  const buf = Buffer.from(await r.arrayBuffer());
  const ct = (r.headers.get('content-type') || '').toLowerCase();

  let text;
  if (ct.includes('iso-8859') || ct.includes('latin1')) {
    text = buf.toString('latin1');
  } else {
    const utf8 = buf.toString('utf8');
    const lat1 = buf.toString('latin1');
    const bads = (s) => (s.match(/\uFFFD/g) || []).length;
    text = bads(lat1) < bads(utf8) ? lat1 : utf8;
  }
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return JSON.parse(text);
}

function gunzipIfNeeded(buf) { return isGzip(buf) ? zlib.gunzipSync(buf) : buf; }

async function tarEntries(buf) {
  const tarBuf = gunzipIfNeeded(buf);
  const out = [];
  await new Promise((resolve, reject) => {
    const extract = tar.extract();
    extract.on('entry', (hdr, stream, next) => {
      const chunks = [];
      stream.on('data', (c) => chunks.push(c));
      stream.on('end', () => {
        const buffer = Buffer.concat(chunks);
        const sha1 = crypto.createHash('sha1').update(buffer).digest('hex');
        out.push({ name: hdr.name, size: buffer.length, buffer, sha1 });
        next();
      });
      stream.on('error', reject);
    });
    extract.on('finish', resolve);
    extract.on('error', reject);
    extract.end(tarBuf);
  });
  return out;
}

function decodeToString(b) {
  try { return b.toString('utf8'); } catch { return b.toString('latin1'); }
}

// ========================= PARSEO CAP v1.2 =========================

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  textNodeName: '#text',
  trimValues: true,
});

function asArray(x) { return Array.isArray(x) ? x : x == null ? [] : [x]; }

function parseCapXml(xmlText) {
  const root = parser.parse(xmlText);
  const alerts = asArray(root?.alert || root?.['cap:alert']);
  return alerts.map((alert) => {
    const header = {
      identifier: alert?.identifier ?? null,
      sender: alert?.sender ?? null,
      sent: alert?.sent ?? null,
      status: alert?.status ?? null,
      msgType: alert?.msgType ?? null,
      scope: alert?.scope ?? null,
    };

    const infoList = asArray(alert?.info).map((info) => {
      const category = asArray(info?.category).map(String);
      const responseType = asArray(info?.responseType).map(String);

      const parameters = asArray(info?.parameter).map((p) => ({
        valueName: p?.valueName ?? p?.['@_valueName'] ?? p?.name ?? null,
        value: p?.value ?? p?.['#text'] ?? null,
      }));

      const eventCode = asArray(info?.eventCode).map((ec) => ({
        name: ec?.name ?? ec?.['@_name'] ?? null,
        value: ec?.value ?? ec?.['#text'] ?? null,
      }));

      const areas = asArray(info?.area).map((a) => ({
        areaDesc: a?.areaDesc ?? null,
        altitude: a?.altitude ?? null,
        ceiling: a?.ceiling ?? null,
        polygons: asArray(a?.polygon).map(String),
        circles: asArray(a?.circle).map(String),
        geocodes: asArray(a?.geocode).map((g) => ({
          valueName: g?.valueName ?? g?.['@_valueName'] ?? null,
          value: g?.value ?? g?.['#text'] ?? null,
        })),
      }));

      return {
        language: info?.language ?? null,
        category,
        event: info?.event ?? null,
        responseType,
        urgency: info?.urgency ?? null,
        severity: info?.severity ?? null,
        certainty: info?.certainty ?? null,
        effective: info?.effective ?? null,
        onset: info?.onset ?? null,
        expires: info?.expires ?? null,
        headline: info?.headline ?? null,
        description: info?.description ?? null,
        instruction: info?.instruction ?? null,
        web: info?.web ?? null,
        contact: info?.contact ?? null,
        parameters,
        eventCode,
        areas,
      };
    });

    return { header, info: infoList };
  });
}

// ========================= MATCH POR ZONA =========================

function fileMatchesZonaByName(fileName, zona) { return fileName.includes(zona); }

function alertHasZonaByGeocode(parsedAlert, zona) {
  for (const inf of parsedAlert.info) {
    for (const area of inf.areas) {
      for (const g of area.geocodes) {
        if (String(g.value || '').includes(zona)) return true;
      }
    }
  }
  return false;
}

// ========================= REFRESCO DESDE AEMET (por área) =========================
//
// Descarga el “ultimoelaborado” del área, extrae TAR/XML, parsea CAP, y
// ACUMULA en cacheZona todos los avisos de cada zona detectada.
//
async function refreshArea(area) {
  if (!AEMET_API_KEY) throw new Error('Falta AEMET_API_KEY en el entorno.');
  const urlCatalogo = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${area}?api_key=${encodeURIComponent(AEMET_API_KEY)}`;

  // 1) HATEOAS (datos/metadatos)
  const cat = await tryFetchJSON(urlCatalogo);
  const urlDatos = cat?.datos;
  const urlMetadatos = cat?.metadatos || null;
  if (!urlDatos) throw new Error('Respuesta de AEMET sin "datos".');

  // 2) Descargar TAR/XML
  const dataBuf = await tryFetchBuffer(urlDatos);

  // 3) Extraer (TAR o XML simple)
  let entries = [];
  let isTar = true;
  try {
    entries = await tarEntries(dataBuf);
  } catch {
    isTar = false;
  }

  // 4) Recopilar ficheros + construir/ACUMULAR payload por zona
  const ficheros = [];
  const nowIso = new Date().toISOString();
  const zonasActualizadas = new Set();
  let metadatos = null;

  // Auxiliares de acumulación/deduplicado
  const dedupKey = (a) => `${a.file}::${a.header?.identifier || ''}`;

  // Inserta/Acumula en cacheZona para una zona concreta
  function upsertZona(zona, nuevosAvisos, fileList) {
    if (!zona || nuevosAvisos.length === 0) return;

    const newPayload = {
      query: {
        zona,
        area,
        url_catalogo: urlCatalogo,
        url_datos: urlDatos,
        url_metadatos: urlMetadatos,
        last_success_at: nowIso
      },
      metadatos: null, // se rellenará al final (una vez por área)
      ficheros: fileList,
      avisos: nuevosAvisos
    };

    const existing = cacheZona.get(zona);

    if (existing?.payload) {
      // 1) Unir ficheros (dedupe por 'name')
      const filesByName = new Map();
      for (const f of [...existing.payload.ficheros, ...newPayload.ficheros]) {
        if (!filesByName.has(f.name)) filesByName.set(f.name, f);
      }

      // 2) Unir avisos (dedupe por 'file' + 'identifier')
      const avisosByKey = new Map();
      for (const a of [...existing.payload.avisos, ...newPayload.avisos]) {
        avisosByKey.set(dedupKey(a), a);
      }

      // 3) Mantener la query más reciente (last_success_at del último fichero)
      const mergedPayload = {
        ...existing.payload,
        query: { ...existing.payload.query, ...newPayload.query },
        metadatos: existing.payload.metadatos ?? null,
        ficheros: Array.from(filesByName.values()),
        avisos: Array.from(avisosByKey.values())
      };

      cacheZona.set(zona, { payload: mergedPayload, fetchedAt: nowMs(), stale: false });
    } else {
      cacheZona.set(zona, { payload: newPayload, fetchedAt: nowMs(), stale: false });
    }

    zonasActualizadas.add(zona);
  }

  if (isTar) {
    // Guardamos listado de ficheros del TAR (para trazabilidad)
    for (const ent of entries) {
      ficheros.push({ name: ent.name, size: ent.size, sha1: ent.sha1, matched_by: null });
    }

    // Procesamos cada entrada XML del TAR
    for (const e of entries) {
      if (!e.name.toLowerCase().endsWith('.xml')) continue;

      const fileName = e.name;
      const xml = decodeToString(e.buffer);
      const parsedList = parseCapXml(xml);

      // Detectar posibles zonas: por nombre y por geocódigo
      const posiblesZonas = new Set();
      const nameMatches = fileName.match(/\d{6}/g) || [];
      nameMatches.forEach(z => posiblesZonas.add(z));

      for (const pa of parsedList) {
        for (const inf of pa.info) {
          for (const area of inf.areas) {
            for (const g of area.geocodes) {
              const ms = String(g.value || '').match(/\d{6}/g) || [];
              ms.forEach(z => posiblesZonas.add(z));
            }
          }
        }
      }

      // Para cada zona detectada, recolectamos los avisos que la contienen
      for (const zona of posiblesZonas) {
        const avisos = [];
        const matchedByName = fileMatchesZonaByName(fileName, zona);

        for (const pa of parsedList) {
          const matchedGeo = alertHasZonaByGeocode(pa, zona);
          if (matchedByName || matchedGeo) {
            // Adjuntamos el XML bruto y la referencia al fichero origen
            avisos.push({ file: fileName, ...pa, raw_xml: xml });
          }
        }

        // ACUMULAR en caché (no sobrescribir) para esta zona
        if (avisos.length > 0) {
          upsertZona(zona, avisos, ficheros);
        }
      }
    }
  } else {
    // XML directo (sin TAR)
    const xml = decodeToString(dataBuf);
    const parsedList = parseCapXml(xml);

    const ficherosXml = [{ name: 'datos.xml', size: xml.length, sha1: null, matched_by: 'geocode' }];

    const posiblesZonas = new Set();
    for (const pa of parsedList) {
      for (const inf of pa.info) {
        for (const area of inf.areas) {
          for (const g of area.geocodes) {
            const ms = String(g.value || '').match(/\d{6}/g) || [];
            ms.forEach(z => posiblesZonas.add(z));
          }
        }
      }
    }

    for (const zona of posiblesZonas) {
      const avisos = [];
      for (const pa of parsedList) {
        const matched = alertHasZonaByGeocode(pa, zona);
        if (matched) avisos.push({ file: 'datos.xml', ...pa, raw_xml: xml, matched_by: 'geocode' });
      }
      if (avisos.length > 0) {
        upsertZona(zona, avisos, ficherosXml);
      }
    }
  }

  // 5) Metadatos (una vez por área) y propagar a zonas actualizadas
  if (cat?.metadatos) {
    try { metadatos = await fetchJSONSmart(cat.metadatos); } catch { /* opcional */ }
  }
  if (metadatos) {
    for (const zona of zonasActualizadas) {
      const entry = cacheZona.get(zona);
      if (entry?.payload) entry.payload.metadatos = metadatos;
    }
  }

  return { area, zonasActualizadas: Array.from(zonasActualizadas), filesCount: isTar ? entries.length : 1 };
}

// ========================= AUTH PARA ENDPOINTS ADMIN =========================

function requireCronToken(req, res, next) {
  const tok = req.headers['x-cron-token'];
  if (!CRON_TOKEN || tok !== CRON_TOKEN) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

// ========================= ENDPOINTS =========================

// Estado básico
app.get('/', (_, res) => {
  res.type('text/plain').send('AEMET avisos por zona – API de caché (cron/admin + consulta)');
});

// Health: muestra tamaño de caché y zonas de ejemplo
app.get('/health', (_, res) => {
  const sample = Array.from(cacheZona.keys()).slice(0, 5);
  res.json({
    ok: true,
    zones_cached: cacheZona.size,
    sample_zones: sample,
    ttl_seconds: CACHE_TTL_SECONDS
  });
});

// --- ADMIN: refrescar un área concreta (POST /admin/refresh?area=NN) ---
app.post('/admin/refresh', requireCronToken, async (req, res) => {
  try {
    const area = String(req.query.area || '').trim();
    if (!/^\d{2}$/.test(area)) {
      const e = new Error('Parámetro "area" inválido. Debe ser 2 dígitos (p.ej. 61).');
      e.status = 400;
      throw e;
    }
    const r = await refreshArea(area);
    res.json({ ok: true, ...r });
  } catch (err) {
    res.status(err.status || 500).json({ ok: false, error: String(err.message || err) });
  }
});

// --- ADMIN: refrescar todas las áreas configuradas (POST /admin/refresh-all) ---
app.post('/admin/refresh-all', requireCronToken, async (req, res) => {
  try {
    const areas = (req.body?.areas && Array.isArray(req.body.areas) ? req.body.areas : AREAS);
    if (!areas || areas.length === 0) {
      return res.status(400).json({ ok: false, error: 'No hay áreas definidas. Usa body {"areas":[..]} o variable AREAS.' });
    }

    const results = [];
    for (const a of areas) {
      const a2 = String(a).padStart(2, '0');
      try {
        const r = await refreshArea(a2);
        results.push({ area: a2, ok: true, zonas: r.zonasActualizadas.length });
        // Pequeño respiro para no saturar AEMET
        await sleep(250);
      } catch (e) {
        results.push({ area: a2, ok: false, error: String(e.message || e) });
      }
    }
    res.json({ ok: true, results });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

// --- PÚBLICO: consulta por zona (GET /avisos?zona=XXXXXX) ---
app.get('/avisos', async (req, res) => {
  try {
    const zona = String(req.query.zona || '').trim();
    assertZona(zona);

    const entry = cacheZona.get(zona);
    if (!entry) {
      // No hay dato en caché para esa zona; NO se consulta AEMET aquí
      return res.status(503).json({ error: 'cache_miss', zona });
    }

    const expired = isExpired(entry);
    const payload = {
      ...entry.payload,
      stale: Boolean(entry.stale || expired),
      cache: {
        fetched_at: new Date(entry.fetchedAt).toISOString(),
        ttl_seconds: CACHE_TTL_SECONDS,
        expired
      }
    };
    return res.json(payload);
  } catch (err) {
    const msg = String(err.message || err);
    const status = err.status || 500;
    return res.status(status).json({ error: msg, status });
  }
});

app.listen(PORT, () => {
  console.log(`AEMET avisos por zona – caché escuchando en :${PORT}`);
});
