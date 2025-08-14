// server.js
// Microservicio: Avisos AEMET por zona (CAP v1.2) – con caché del último OK y reintentos
// --------------------------------------------------------------------------------------
// Cambios clave respecto al original:
// 1) Se cachea SIEMPRE el último resultado exitoso en memoria (lastOk) y su marca de tiempo (lastSuccessAt).
// 2) Si la consulta a AEMET falla, se reintenta un par de veces con backoff y timeout.
// 3) Si aun así falla, se devuelve el último OK ("stale: true") para no romper llamadas desde MT Neo.
// 4) Se añade en la respuesta "last_success_at": hora del último intento positivo (ISO).
//
// Resto de comportamiento: igual que tu versión anterior (validación de zona, parseo CAP, etc):contentReference[oaicite:2]{index=2}.
// Dependencias y entrypoint: iguales a tu package.json actual:contentReference[oaicite:3]{index=3}.
//
// --------------------------------------------------------------------------------------

import express from 'express';
import { fetch } from 'undici';
import * as zlib from 'zlib';
import tar from 'tar-stream';
import crypto from 'crypto';
import { XMLParser } from 'fast-xml-parser';

const app = express();
const PORT = process.env.PORT || 3000;
const UA = 'MT-Neo-Avisos-Zona/1.1'; // sube versión interna del UA

app.use(express.json({ limit: '4mb' }));

// =================== Estado en memoria (caché del último éxito) ===================

// lastOk: último payload de respuesta correcto (tal cual se envió a MT Neo)
// lastSuccessAt: ISO string con la hora del último éxito
let lastOk = null;
let lastSuccessAt = null;

// =================== Utilidades generales ========================================

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

// --- Timeout + reintentos con backoff (mejora respecto a fetchJSON/fetchBuffer) ---

async function fetchWithTimeout(url, options = {}, ms = 10000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try {
    return await fetch(url, { ...options, signal: ac.signal });
  } finally {
    clearTimeout(t);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Reintenta N veces con backoff exponencial + jitter ligero
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

// --- Lectores originales (mantenidos por compatibilidad interna y metadatos) ---

async function fetchJSONSmart(url, headers = {}) {
  // Igual que tu versión: intenta decodificación robusta (UTF-8/Latin-1):contentReference[oaicite:4]{index=4}
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

function gunzipIfNeeded(buf) {
  return isGzip(buf) ? zlib.gunzipSync(buf) : buf;
}

async function tarEntries(buf) {
  // Igual que antes: devuelve [{name, buffer, size, sha1}] desde TAR/TAR.GZ:contentReference[oaicite:5]{index=5}
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
  // Igual que antes: UTF-8 por defecto, Latin-1 de reserva:contentReference[oaicite:6]{index=6}
  try {
    return b.toString('utf8');
  } catch {
    return b.toString('latin1');
  }
}

// =================== Parseo CAP v1.2 (igual que original) ========================

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  textNodeName: '#text',
  trimValues: true,
});

function asArray(x) {
  return Array.isArray(x) ? x : x == null ? [] : [x];
}

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

function fileMatchesZonaByName(fileName, zona) {
  return fileName.includes(zona);
}

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

// =================== Endpoints ===================================================

app.get('/', (_, res) => {
  res.type('text/plain').send('AEMET avisos por zona – OK (con caché y reintentos)');
});

app.get('/avisos', async (req, res) => {
  try {
    const zona = String(req.query.zona || '').trim();
    assertZona(zona);

    // api_key por query o variable de entorno
    const apiKey = String(req.query.api_key || process.env.AEMET_API_KEY || '').trim();
    if (!apiKey) {
      const e = new Error('Falta api_key (query ?api_key=... o variable de entorno AEMET_API_KEY).');
      e.status = 400;
      throw e;
    }

    // Área = 2 primeros dígitos de la zona (ej. 61 -> Andalucía):contentReference[oaicite:7]{index=7}
    const area = zona.substring(0, 2);

    // 1) HATEOAS: obtener URL real de datos, con reintentos y timeout
    const urlCatalogo = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${area}?api_key=${encodeURIComponent(apiKey)}`;
    const cat = await tryFetchJSON(urlCatalogo);
    const urlDatos = cat?.datos;
    const urlMetadatos = cat?.metadatos || null;
    if (!urlDatos) {
      const e = new Error('Respuesta de AEMET sin "datos".');
      e.status = 502;
      throw e;
    }

    // 2) Descarga de datos (TAR/TAR.GZ o, excepcionalmente, XML directo) con reintentos
    const dataBuf = await tryFetchBuffer(urlDatos);

    // 3) Extraer TAR o tratar como XML directo (igual que antes)
    let entries = [];
    let isTar = true;
    try {
      entries = await tarEntries(dataBuf);
    } catch {
      isTar = false;
    }

    const ficheros = [];
    const avisos = [];

    if (isTar) {
      // --- TAR con múltiples XML CAP ---
      for (const ent of entries) {
        ficheros.push({
          name: ent.name,
          size: ent.size,
          sha1: ent.sha1,
          matched_by: fileMatchesZonaByName(ent.name, zona) ? 'file_name' : null,
        });
      }

      // 3.a) Filtrar por nombre de fichero
      let objetivos = entries.filter(
        (e) => fileMatchesZonaByName(e.name, zona) && e.name.toLowerCase().endsWith('.xml')
      );

      // 3.b) Si no hay por nombre, parsear todos y filtrar por geocode
      if (objetivos.length === 0) {
        for (const e of entries) {
          if (!e.name.toLowerCase().endsWith('.xml')) continue;
          const xml = decodeToString(e.buffer);
          const parsedList = parseCapXml(xml);
          for (const pa of parsedList) {
            if (alertHasZonaByGeocode(pa, zona)) {
              avisos.push({ file: e.name, ...pa, raw_xml: xml });
              const ff = ficheros.find((x) => x.name === e.name);
              if (ff && !ff.matched_by) ff.matched_by = 'geocode';
            }
          }
        }
      } else {
        // Parsear únicamente los objetivos (por nombre)
        for (const e of objetivos) {
          const xml = decodeToString(e.buffer);
          const parsedList = parseCapXml(xml);
          for (const pa of parsedList) {
            avisos.push({ file: e.name, ...pa, raw_xml: xml });
          }
        }
      }
    } else {
      // --- XML directo (caso raro) ---
      const xml = decodeToString(dataBuf);
      const parsedList = parseCapXml(xml);
      for (const pa of parsedList) {
        const matched = alertHasZonaByGeocode(pa, zona);
        avisos.push({ file: 'datos.xml', ...pa, raw_xml: xml, matched_by: matched ? 'geocode' : null });
      }
    }

    // 4) Recuperar metadatos (opcional) con decodificación robusta
    let metadatos = null;
    if (urlMetadatos) {
      try {
        metadatos = await fetchJSONSmart(urlMetadatos);
      } catch {
        // Ignorar fallo de metadatos
      }
    }

    // 5) Construir respuesta y guardar en caché de último éxito
    const nowIso = new Date().toISOString();
    const payload = {
      query: {
        zona,
        area,
        url_catalogo: urlCatalogo,
        url_datos: urlDatos,
        url_metadatos: urlMetadatos,
        last_success_at: nowIso // <-- NUEVO: hora del último éxito
      },
      metadatos,
      ficheros,
      avisos,
    };

    lastOk = payload;
    lastSuccessAt = nowIso;

    return res.json(payload);

  } catch (err) {
    // Clasificación simple del error para status coherente
    const msg = String(err.message || err);
    let status = err.status || 500;
    if (/aborted|The operation was aborted|timeout/i.test(msg)) status = 504;        // timeout a AEMET
    else if (/HTTP \d+ en https?:\/\/opendata\.aemet\.es/i.test(msg)) status = 502;  // error upstream catálogo
    else if (/al descargar datos \(TAR\/XML\)/i.test(msg)) status = 502;            // error upstream datos

    // Degradación: si tenemos último OK, lo servimos marcando stale=true
    if (lastOk) {
      const safe = {
        ...lastOk,
        stale: true,
        error: msg,
        // también exponemos explícitamente el último éxito por si el cliente lo usa suelto
        last_success_at: lastSuccessAt
      };
      return res.status(200).json(safe);
    }

    // Si no hay caché previa, devolvemos el error
    return res.status(status).json({ error: msg, status });
  }
});

// Salud: incluye la hora del último éxito
app.get('/health', (_, res) => {
  res.json({ ok: true, last_success_at: lastSuccessAt });
});

app.listen(PORT, () => {
  console.log(`AEMET avisos por zona (con caché lastOk) escuchando en :${PORT}`);
});
