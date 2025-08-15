// server.js
// Microservicio AEMET – caché por zona con refresco vía cron (admin) y lectura pública sin llamadas a AEMET
// --------------------------------------------------------------------------------------------------------
// Objetivo solicitado:
//   1) Separar CAPTURA (AEMET vía /admin/*, la invoca el cron de Render) de CONSULTA pública (/avisos),
//      que solo lee de caché y NUNCA llama a AEMET.
//   2) Para cada zona (6 dígitos), ACUMULAR TODOS los avisos presentes en el TAR/XML “ultimoelaborado”,
//      sin perder ninguno por sobrescritura, con deduplicación por (file + header.identifier).
//   3) La respuesta pública mantiene la ESTRUCTURA original (compatible con MT Neo):
//        { query, ficheros, avisos, stale, cache }
//      PERO eliminando campos pesados:
//        - NO devolver `metadatos`
//        - NO incluir `areas` dentro de `info`
//        - NO incluir `raw_xml`
//
//   4) Cambios pedidos (este archivo ya los integra):
//      - NO procesar ficheros “generales CCAA” (patrón AFAZ<AREA>VV…)
//      - EXCLUIR avisos de nivel VERDE (solo amarillo/naranja/rojo)
//      - EXCLUIR avisos genéricos CCAA por titular (“… CCAA”)
//      - Opcional: en `ficheros`, listar solo los que aportaron avisos tras filtros
// --------------------------------------------------------------------------------------------------------

import express from 'express';
import { fetch } from 'undici';
import * as zlib from 'zlib';
import tar from 'tar-stream';
import crypto from 'crypto';
import { XMLParser } from 'fast-xml-parser';

const app = express();
const PORT = process.env.PORT || 3000;
const UA = 'MT-Neo-Avisos-Zona/2.2';
const AEMET_API_KEY = process.env.AEMET_API_KEY || '';
const CRON_TOKEN = process.env.RENDER_CRON_TOKEN || '';
const CACHE_TTL_SECONDS = parseInt(process.env.CACHE_TTL_SECONDS || '1200', 10); // 20 min por defecto
const AREAS = (process.env.AREAS || '').split(',').map(s => s.trim()).filter(Boolean); // p.ej. "61,62,63,..."

app.use(express.json({ limit: '4mb' }));

// ========================= CACHÉ EN MEMORIA (por zona) =========================
//
// Estructura de cada entrada:
//   {
//     payload: { query, ficheros[], avisos[] },   // (SIN metadatos)
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

// ========================= PARSEO CAP v1.2 (normalizado) =========================

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  textNodeName: '#text',
  trimValues: true,
});

function asArray(x) { return Array.isArray(x) ? x : x == null ? [] : [x]; }

/**
 * Parsea un XML CAP y lo normaliza en un array de objetos:
 *  { header, info[] }
 * Donde cada info NO incluye "areas" (se eliminan) para aligerar payload.
 */
function parseCapXmlWithoutAreas(xmlText) {
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

      // ⚠️ Importante: NO devolvemos "areas" para aligerar la respuesta.
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
        eventCode
        // sin areas
      };
    });

    return { header, info: infoList };
  });
}

// ========================= MATCH POR ZONA =========================

function fileMatchesZonaByName(fileName, zona) { return fileName.includes(zona); }

// Dado un aviso ya parseado CON AREAS, detecta si contiene la zona.
// Aquí solo para matching interno; en la salida pública NO incluimos areas.
function alertHasZonaByGeocode_WITH_AREAS(parsedAlert, zona) {
  const infos = asArray(parsedAlert.info);
  for (const inf of infos) {
    const areas = asArray(inf.areas || []);
    for (const area of areas) {
      const geocodes = asArray(area.geocodes || area.geocode || []);
      for (const g of geocodes) {
        const value = g?.value ?? g?.['#text'] ?? '';
        if (String(value).includes(zona)) return true;
      }
    }
  }
  return false;
}

// ========================= FILTROS PERSONALIZADOS =========================
//
// Requisitos del usuario: excluir
//  - Ficheros “generales CCAA” (patrón AFAZ<AREA>VV…)
//  - Avisos de nivel VERDE
//  - Avisos genéricos CCAA por titular (“… CCAA”)
// --------------------------------------------------------------------------

/** Heurística para identificar ficheros agregados de CCAA (no se procesan) */
function isGenericCCAAFileName(fileName) {
  // AFAZ<AREA>VV... => ficheros agregados de Comunidad Autónoma
  return /AFAZ\d{2}VV/i.test(fileName || '');
}

/** Devuelve el nivel de Meteoalerta a partir de info.parameters */
function getAemetLevelFromInfo(info) {
  const params = Array.isArray(info?.parameters) ? info.parameters : [];
  for (const p of params) {
    const name = String(p?.valueName || '').toLowerCase();
    if (name === 'aemet-meteoalerta nivel') {
      return String(p?.value || '').toLowerCase(); // 'verde' | 'amarillo' | 'naranja' | 'rojo'
    }
  }
  return null;
}

/** Verdadero si ALGUNA info es no-verde (amarillo/naranja/rojo) o la severidad CAP es >= Moderate */
function alertHasNonGreenLevel(infos) {
  for (const i of (Array.isArray(infos) ? infos : [])) {
    const lvl = getAemetLevelFromInfo(i);
    if (lvl && lvl !== 'verde') return true;
  }
  for (const i of (Array.isArray(infos) ? infos : [])) {
    const sev = String(i?.severity || '').toLowerCase(); // Minor, Moderate, Severe, Extreme
    if (sev === 'moderate' || sev === 'severe' || sev === 'extreme') return true;
  }
  return false;
}

/** Verdadero si el titular parece genérico de CCAA */
function alertLooksGenericCCAA(infos) {
  for (const i of (Array.isArray(infos) ? infos : [])) {
    const hl = String(i?.headline || '').toLowerCase();
    if (hl.includes('ccaa')) return true; // p.ej. “Aviso ... CCAA”
  }
  return false;
}

// ========================= REFRESCO DESDE AEMET (por área) =========================
//
// Descarga el “ultimoelaborado” del área, extrae TAR/XML, y ACUMULA en cacheZona
// todos los avisos de cada zona detectada, SIN metadatos/areas/raw_xml en el payload final.
// Aplica filtros personalizados (no VV, no verdes, no genéricos CCAA).
//
async function refreshArea(area) {
  if (!AEMET_API_KEY) throw new Error('Falta AEMET_API_KEY en el entorno.');
  const urlCatalogo = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${area}?api_key=${encodeURIComponent(AEMET_API_KEY)}`;

  // 1) HATEOAS (datos/metadatos)
  const cat = await tryFetchJSON(urlCatalogo);
  const urlDatos = cat?.datos;
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

  // 4) Recopilar ficheros + construir/ACUMULAR payload por zona (sin metadatos/areas/raw_xml)
  const ficheros = [];
  const nowIso = new Date().toISOString();

  // 🔎 Para limpiar `ficheros`: guardamos por zona qué ficheros han aportado avisos tras filtros
  const usedFilesByZona = new Map(); // zona -> Set(fileName)

  // Auxiliar de deduplicado por 'file + header.identifier'
  const dedupKey = (a) => `${a.file}::${a.header?.identifier || ''}`;

  // Inserta/Acumula en cacheZona para una zona concreta
  function upsertZona(zona, nuevosAvisos, fileList, ctxQuery) {
    if (!zona || nuevosAvisos.length === 0) return;

    // Limpiar `ficheros`: solo los que realmente aportaron avisos a esta zona en esta pasada
    const usedSet = usedFilesByZona.get(zona) || new Set();
    const ficherosFiltrados = fileList.filter(f => usedSet.has(f.name));

    const newPayload = {
      query: { ...ctxQuery, zona },
      // SIN metadatos en la salida pública
      ficheros: ficherosFiltrados,
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
        ficheros: Array.from(filesByName.values()),
        avisos: Array.from(avisosByKey.values())
      };

      cacheZona.set(zona, { payload: mergedPayload, fetchedAt: nowMs(), stale: false });
    } else {
      cacheZona.set(zona, { payload: newPayload, fetchedAt: nowMs(), stale: false });
    }
  }

  const baseQuery = {
    area,
    url_catalogo: urlCatalogo,
    url_datos: urlDatos,
    url_metadatos: cat?.metadatos || null, // se mantiene para trazabilidad en query
    last_success_at: nowIso
  };

  if (isTar) {
    // Guardamos listado de ficheros del TAR (para trazabilidad), pero filtraremos más tarde
    for (const ent of entries) {
      ficheros.push({ name: ent.name, size: ent.size, sha1: ent.sha1, matched_by: null });
    }

    // Procesamos cada entrada XML del TAR
    for (const e of entries) {
      if (!e.name.toLowerCase().endsWith('.xml')) continue;

      // ⛔️ Saltar ficheros generales CCAA (VV) directamente (ahorro CPU/I/O)
      if (isGenericCCAAFileName(e.name)) continue;

      const fileName = e.name;
      const xml = decodeToString(e.buffer);

      // Parseo doble: con áreas (solo matching) y sin áreas (salida ligera)
      const parsedWithAreas = parseCap_FOR_MATCHING(xml);
      const parsedWithoutAreas = parseCapXmlWithoutAreas(xml);

      // Detectar posibles zonas candidatas por nombre y geocódigos
      const posiblesZonas = new Set();
      const nameMatches = fileName.match(/\d{6}/g) || [];
      nameMatches.forEach(z => posiblesZonas.add(z));

      for (const pa of parsedWithAreas) {
        if (!pa) continue;
        for (const inf of asArray(pa.info)) {
          for (const areaObj of asArray(inf.areas || [])) {
            for (const g of asArray(areaObj.geocodes || areaObj.geocode || [])) {
              const ms = String(g?.value ?? g?.['#text'] ?? '').match(/\d{6}/g) || [];
              ms.forEach(z => posiblesZonas.add(z));
            }
          }
        }
      }

      // Para cada zona detectada, recolectamos avisos que la contengan (y pasen filtros)
      for (const zona of posiblesZonas) {
        const avisos = [];
        const matchedByName = fileMatchesZonaByName(fileName, zona);

        for (let i = 0; i < parsedWithoutAreas.length; i++) {
          const aSinAreas = parsedWithoutAreas[i];
          const aConAreas = parsedWithAreas[i]; // índice homólogo

          // ¿Este aviso contiene la zona? (por nombre de fichero o por geocódigo)
          const matchedGeo = aConAreas ? alertHasZonaByGeocode_WITH_AREAS(aConAreas, zona) : false;
          if (!(matchedByName || matchedGeo)) continue;

          // ⛔️ Filtrado: excluir VERDE
          if (!alertHasNonGreenLevel(aSinAreas?.info)) continue;

          // ⛔️ Filtrado: excluir genéricos CCAA por titular
          if (alertLooksGenericCCAA(aSinAreas?.info)) continue;

          // ✅ Pasa filtros → añadimos aviso y marcamos fichero como usado para esta zona
          avisos.push({ file: fileName, ...aSinAreas });
          if (!usedFilesByZona.has(zona)) usedFilesByZona.set(zona, new Set());
          usedFilesByZona.get(zona).add(fileName);
        }

        if (avisos.length > 0) {
          upsertZona(zona, avisos, ficheros, baseQuery);
        }
      }
    }
  } else {
    // XML directo (sin TAR)
    const xml = decodeToString(dataBuf);

    // Parseo doble: con áreas (solo matching) y sin áreas (salida ligera)
    const parsedWithAreas = parseCap_FOR_MATCHING(xml);
    const parsedWithoutAreas = parseCapXmlWithoutAreas(xml);

    // Como no hay nombre de fichero real, usamos un nombre lógico
    const fileName = 'datos.xml';
    const ficherosXml = [{ name: fileName, size: xml.length, sha1: null, matched_by: 'geocode' }];

    const posiblesZonas = new Set();
    for (const pa of parsedWithAreas) {
      for (const inf of asArray(pa.info)) {
        for (const areaObj of asArray(inf.areas || [])) {
          for (const g of asArray(areaObj.geocodes || areaObj.geocode || [])) {
            const ms = String(g?.value ?? g?.['#text'] ?? '').match(/\d{6}/g) || [];
            ms.forEach(z => posiblesZonas.add(z));
          }
        }
      }
    }

    for (const zona of posiblesZonas) {
      const avisos = [];
      for (let i = 0; i < parsedWithoutAreas.length; i++) {
        const aSinAreas = parsedWithoutAreas[i];
        const aConAreas = parsedWithAreas[i];
        const matched = aConAreas ? alertHasZonaByGeocode_WITH_AREAS(aConAreas, zona) : false;
        if (!matched) continue;

        // ⛔️ Excluir VERDE
        if (!alertHasNonGreenLevel(aSinAreas?.info)) continue;

        // ⛔️ Excluir genéricos CCAA
        if (alertLooksGenericCCAA(aSinAreas?.info)) continue;

        // ✅ Pasa filtros
        avisos.push({ file: fileName, ...aSinAreas });
        if (!usedFilesByZona.has(zona)) usedFilesByZona.set(zona, new Set());
        usedFilesByZona.get(zona).add(fileName);
      }
      if (avisos.length > 0) {
        upsertZona(zona, avisos, ficherosXml, baseQuery);
      }
    }
  }

  // Nota: `ficheros` ya se depura por zona dentro de upsertZona usando usedFilesByZona.
  return { area, filesCount: isTar ? (entries?.length || 0) : 1 };
}

// ----- Parser auxiliar SOLO para matching (incluye areas), no se expone en salida pública -----
function parseCap_FOR_MATCHING(xmlText) {
  const p2 = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    textNodeName: '#text',
    trimValues: true,
  });
  const root = p2.parse(xmlText);
  const alerts = asArray(root?.alert || root?.['cap:alert']);
  // Mantener info.areas y geocodes tal cual para verificar zona
  return alerts.map((alert) => ({
    header: {
      identifier: alert?.identifier ?? null,
      sender: alert?.sender ?? null,
      sent: alert?.sent ?? null,
      status: alert?.status ?? null,
      msgType: alert?.msgType ?? null,
      scope: alert?.scope ?? null,
    },
    info: asArray(alert?.info).map((info) => ({
      language: info?.language ?? null,
      category: asArray(info?.category).map(String),
      event: info?.event ?? null,
      responseType: asArray(info?.responseType).map(String),
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
      parameters: asArray(info?.parameter).map((p) => ({
        valueName: p?.valueName ?? p?.['@_valueName'] ?? p?.name ?? null,
        value: p?.value ?? p?.['#text'] ?? null,
      })),
      eventCode: asArray(info?.eventCode).map((ec) => ({
        name: ec?.name ?? ec?.['@_name'] ?? null,
        value: ec?.value ?? ec?.['#text'] ?? null,
      })),
      // Aquí sí conservamos areas para poder detectar la zona.
      areas: asArray(info?.area).map((a) => ({
        areaDesc: a?.areaDesc ?? null,
        polygons: asArray(a?.polygon).map(String),
        geocodes: asArray(a?.geocode).map((g) => ({
          valueName: g?.valueName ?? g?.['@_valueName'] ?? null,
          value: g?.value ?? g?.['#text'] ?? null,
        })),
      })),
    })),
  }));
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
        results.push({ area: a2, ok: true, filesCount: r.filesCount });
        await sleep(250); // pequeño respiro para no saturar AEMET
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
// Devuelve estructura compatible con el primer servicio: { query, ficheros, avisos, stale, cache }
// (SIN metadatos, SIN areas en info, SIN raw_xml)
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
      ...entry.payload, // { query, ficheros, avisos }
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


// --- PÚBLICO: estado por áreas (GET /areas/status[?area=NN][&include_empty=1]) ---
// Agrupa la caché por zona (cacheZona) en áreas (dos dígitos en query.area) y devuelve métricas.
// - ?area=NN           → filtra por un área concreta (p.ej. 61)
// - &include_empty=1   → incluye áreas definidas en AREAS aunque no tengan zonas en caché
app.get('/areas/status', (req, res) => {
  try {
    const areaFilter = String(req.query.area || '').trim(); // '61', '62', ...
    const includeEmpty = ['1', 'true', 'yes'].includes(String(req.query.include_empty || '').toLowerCase());

    // Agrupamos por área recorriendo la caché por zona
    // cacheZona: Map<zona_6d, { payload:{ query:{ area, last_success_at, ... } }, fetchedAt, stale }>
    const byArea = new Map();

    for (const [zona, entry] of cacheZona.entries()) {
      const area = String(entry?.payload?.query?.area || String(zona).slice(0, 2)).padStart(2, '0');
      if (areaFilter && area !== areaFilter) continue;

      const expired = isExpired(entry);
      const rec = byArea.get(area) || {
        area,
        zones: new Set(),
        zones_count: 0,
        last_success_at_latest: null,
        last_success_at_earliest: null,
        fetched_at_latest: null,
        fetched_at_earliest: null,
        expired_any: false,
        expired_all: true
      };

      rec.zones.add(zona);
      rec.zones_count = rec.zones.size;

      // last_success_at viene de la query que guardamos al refrescar el área
      const lsStr = entry?.payload?.query?.last_success_at || null;
      const ls = lsStr ? new Date(lsStr) : null;
      if (ls) {
        rec.last_success_at_latest = !rec.last_success_at_latest || ls > rec.last_success_at_latest ? ls : rec.last_success_at_latest;
        rec.last_success_at_earliest = !rec.last_success_at_earliest || ls < rec.last_success_at_earliest ? ls : rec.last_success_at_earliest;
      }

      // fetchedAt es cuándo metimos en caché esa zona en este servicio
      const fa = new Date(entry.fetchedAt);
      rec.fetched_at_latest = !rec.fetched_at_latest || fa > rec.fetched_at_latest ? fa : rec.fetched_at_latest;
      rec.fetched_at_earliest = !rec.fetched_at_earliest || fa < rec.fetched_at_earliest ? fa : rec.fetched_at_earliest;

      rec.expired_any = rec.expired_any || expired;
      rec.expired_all = rec.expired_all && expired;

      byArea.set(area, rec);
    }

    // Si piden include_empty, añadimos áreas configuradas en AREAS que no tengan caché aún
    if (includeEmpty) {
      for (const a of AREAS) {
        const aa = String(a).padStart(2, '0');
        if (areaFilter && aa !== areaFilter) continue;
        if (!byArea.has(aa)) {
          byArea.set(aa, {
            area: aa,
            zones: new Set(),
            zones_count: 0,
            last_success_at_latest: null,
            last_success_at_earliest: null,
            fetched_at_latest: null,
            fetched_at_earliest: null,
            expired_any: null,  // desconocido (no hay caché)
            expired_all: null
          });
        }
      }
    }

    // Formateamos salida
    const out = Array.from(byArea.values()).map(r => ({
      area: r.area,
      zones_count: r.zones_count,
      sample_zones: Array.from(r.zones).slice(0, 5),
      ttl_seconds: CACHE_TTL_SECONDS, // del entorno
      last_success_at_latest: r.last_success_at_latest ? r.last_success_at_latest.toISOString() : null,
      last_success_at_earliest: r.last_success_at_earliest ? r.last_success_at_earliest.toISOString() : null,
      fetched_at_latest: r.fetched_at_latest ? r.fetched_at_latest.toISOString() : null,
      fetched_at_earliest: r.fetched_at_earliest ? r.fetched_at_earliest.toISOString() : null,
      expired_any: r.expired_any,
      expired_all: r.expired_all
    }));

    // Orden por área ascendente
    out.sort((a, b) => a.area.localeCompare(b.area));

    res.json({ ok: true, areas: out });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});




app.listen(PORT, () => {
  console.log(`AEMET avisos por zona – caché escuchando en :${PORT}`);
});


