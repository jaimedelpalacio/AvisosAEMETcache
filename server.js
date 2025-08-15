// server.js
// Microservicio AEMET: refresco por CRON (admin) y consulta por zona (solo caché)
// ---------------------------------------------------------------------------------
// Objetivo:
//  - Separar la captura desde AEMET (invocada por un cron de Render a /admin/*)
//    de la consulta desde MT Neo (/avisos?zona=XXXXXX), que NO toca AEMET.
//  - Mantener parseo CAP, robustez y trazas mínimas.
// ---------------------------------------------------------------------------------

import express from 'express';
import { fetch } from 'undici';
import * as zlib from 'zlib';
import tar from 'tar-stream';
import crypto from 'crypto';
import { XMLParser } from 'fast-xml-parser';

const app = express();
const PORT = process.env.PORT || 3000;
const UA = 'MT-Neo-Avisos-Zona/2.0'; // versión interna del UA
const AEMET_API_KEY = process.env.AEMET_API_KEY || '';
const CRON_TOKEN = process.env.RENDER_CRON_TOKEN || '';
const CACHE_TTL_SECONDS = parseInt(process.env.CACHE_TTL_SECONDS || '1200', 10); // 20 min por defecto
const AREAS = (process.env.AREAS || '').split(',').map(s => s.trim()).filter(Boolean); // p.ej. "10,11,12,13,..."

app.use(express.json({ limit: '4mb' }));

// =================== Caché en memoria (por zona) ===================
//
// Estructura de cada entrada:
// {
//   payload: { ...respuesta compatible con /avisos... },
//   fetchedAt: Date (ms),
//   stale: boolean  (si el refresco marcó degradación),
// }
// TTL: si (now - fetchedAt) > CACHE_TTL_SECONDS -> se considera expirada.
//
const cacheZona = new Map(); // key: '614102' -> entry

function nowMs() { return Date.now(); }
function isExpired(entry) {
  if (!entry) return true;
  const ageSec = (nowMs() - entry.fetchedAt) / 1000;
  return ageSec > CACHE_TTL_SECONDS;
}

// =================== Utilidades generales (reusadas) ===================

function assertZona(z) {
  if (!/^\d{6}$/.test(z || '')) {
    const e = new Error('Parámetro "zona" inválido. Debe ser 6 dígitos (p.ej. 614102).');
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

// Reintentos con backoff
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
  // Decodificación robusta para metadatos
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

// =================== Parseo CAP v1.2 ===================

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

// =================== Refresco desde AEMET (por área) ===================

async function refreshArea(area) {
  if (!AEMET_API_KEY) throw new Error('Falta AEMET_API_KEY en el entorno.');
  const urlCatalogo = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${area}?api_key=${encodeURIComponent(AEMET_API_KEY)}`;

  // 1) Obtener enlaces HATEOAS
  const cat = await tryFetchJSON(urlCatalogo);
  const urlDatos = cat?.datos;
  const urlMetadatos = cat?.metadatos || null;
  if (!urlDatos) throw new Error('Respuesta de AEMET sin "datos".');

  // 2) Descargar TAR/XML
  const dataBuf = await tryFetchBuffer(urlDatos);

  // 3) Extraer (TAR o XML)
  let entries = [];
  let isTar = true;
  try {
    entries = await tarEntries(dataBuf);
  } catch {
    isTar = false;
  }

  // 4) Recopilar ficheros + avisos por zona detectada
  const ficheros = [];
  const nowIso = new Date().toISOString();
  const zonasActualizadas = new Set();
  let metadatos = null;

  if (isTar) {
    for (const ent of entries) {
      ficheros.push({ name: ent.name, size: ent.size, sha1: ent.sha1, matched_by: null });
    }

    // Intento 1: por nombre de fichero (más rápido)
    for (const e of entries) {
      if (!e.name.toLowerCase().endsWith('.xml')) continue;
      const fileName = e.name;
      const xml = decodeToString(e.buffer);
      const parsedList = parseCapXml(xml);

      // Extraemos todas las zonas presentes (por nombre y por geocódigo)
      // y construimos payload por cada zona.
      const posiblesZonas = new Set();

      // Por nombre: buscar patterns de 6 dígitos típicos en nombre de fichero
      const nameMatches = fileName.match(/\d{6}/g) || [];
      nameMatches.forEach(z => posiblesZonas.add(z));

      // Por geocódigo dentro del XML
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

      // Para cada zona detectada, creamos una respuesta autocontenida
      for (const zona of posiblesZonas) {
        const avisos = [];
        // Si el nombre de fichero ya incluye la zona, marcamos matched_by
        const matchedByName = fileMatchesZonaByName(fileName, zona);
        for (const pa of parsedList) {
          const matchedGeo = alertHasZonaByGeocode(pa, zona);
          if (matchedByName || matchedGeo) {
            avisos.push({ file: fileName, ...pa, raw_xml: xml });
          }
        }

        if (avisos.length > 0) {
          const payload = {
            query: {
              zona,
              area,
              url_catalogo: urlCatalogo,
              url_datos: urlDatos,
              url_metadatos: urlMetadatos,
              last_success_at: nowIso
            },
            metadatos: null, // se rellena abajo una vez por área
            ficheros,
            avisos
          };
          cacheZona.set(zona, { payload, fetchedAt: nowMs(), stale: false });
          zonasActualizadas.add(zona);
        }
      }
    }
  } else {
    // XML directo (raro)
    const xml = decodeToString(dataBuf);
    const parsedList = parseCapXml(xml);
    // Detectar zonas por geocódigo
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
    const ficheros = [{ name: 'datos.xml', size: xml.length, sha1: null, matched_by: 'geocode' }];

    for (const zona of posiblesZonas) {
      const avisos = [];
      for (const pa of parsedList) {
        const matched = alertHasZonaByGeocode(pa, zona);
        if (matched) avisos.push({ file: 'datos.xml', ...pa, raw_xml: xml, matched_by: 'geocode' });
      }
      if (avisos.length > 0) {
        const payload = {
          query: {
            zona,
            area,
            url_catalogo: urlCatalogo,
            url_datos: urlDatos,
            url_metadatos: urlMetadatos,
            last_success_at: nowIso
          },
          metadatos: null,
          ficheros,
          avisos
        };
        cacheZona.set(zona, { payload, fetchedAt: nowMs(), stale: false });
        zonasActualizadas.add(zona);
      }
    }
  }

  // 5) Metadatos (una vez por área)
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

// =================== Auth para endpoints admin ===================

function requireCronToken(req, res, next) {
  const tok = req.headers['x-cron-token'];
  if (!CRON_TOKEN || tok !== CRON_TOKEN) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

// =================== Endpoints ===================

// Salud básica
app.get('/', (_, res) => {
  res.type('text/plain').send('AEMET avisos por zona – API de caché (cron/admin + consulta)');
});

// Estado caché + última actualización visible por zona
app.get('/health', (_, res) => {
  const sample = Array.from(cacheZona.keys()).slice(0, 5);
  res.json({
    ok: true,
    zones_cached: cacheZona.size,
    sample_zones: sample,
    ttl_seconds: CACHE_TTL_SECONDS
  });
});

// --- ADMIN: refrescar un área ---
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

// --- ADMIN: refrescar todas las áreas configuradas ---
app.post('/admin/refresh-all', requireCronToken, async (req, res) => {
  try {
    const areas = (req.body?.areas && Array.isArray(req.body.areas) ? req.body.areas : AREAS);
    if (!areas || areas.length === 0) {
      return res.status(400).json({ ok: false, error: 'No hay áreas definidas. Usa body {areas:[..]} o variable AREAS.' });
    }
    const results = [];
    for (const a of areas) {
      try {
        const r = await refreshArea(String(a).padStart(2, '0'));
        results.push({ area: a, ok: true, zonas: r.zonasActualizadas.length });
        // Pequeño respiro para no saturar AEMET
        await sleep(250);
      } catch (e) {
        results.push({ area: a, ok: false, error: String(e.message || e) });
      }
    }
    res.json({ ok: true, results });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

// --- PÚBLICO: consulta por zona (solo lee de caché) ---
app.get('/avisos', async (req, res) => {
  try {
    const zona = String(req.query.zona || '').trim();
    assertZona(zona);

    const entry = cacheZona.get(zona);
    if (!entry) {
      // No hay dato: no consultamos AEMET aquí
      return res.status(503).json({ error: 'cache_miss', zona });
    }

    // Si está expirada, devolvemos igual pero marcando stale:true para que el cliente decida
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
