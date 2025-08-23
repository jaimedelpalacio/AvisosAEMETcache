// server.js
// Microservicio AEMET – caché por zona con refresco vía cron (admin) y lectura pública sin llamadas a AEMET
// --------------------------------------------------------------------------------------------------------
// Versión: política de REEMPLAZO por zona (no acumula avisos antiguos)
// Cambios clave respecto a la versión anterior:
//   • Estado global de ingesta (ingestState) para enriquecer /health con último intento/OK/error.
//   • /health devuelve: last_refresh_at, last_refresh_ok, last_refresh_error, last_refresh_error_explained.
//   • Endpoints admin instrumentados para actualizar ingestState sin cambiar su contrato.
//   • ⚠️ upsertZona ahora REEMPLAZA la lista de avisos en la zona (no fusiona con lo existente).
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
const AREAS = (process.env.AREAS || '').split(',').map(s => s.trim()).filter(Boolean); // p.ej. "61,62,63"

app.use(express.json({ limit: '4mb' }));

// ========================= CACHÉ EN MEMORIA (por zona) =========================
// Mapa zona (6 dígitos) -> { payload:{ query, ficheros, avisos }, fetchedAt:number(ms), stale:boolean }
const cacheZona = new Map();

// ========================= ESTADO GLOBAL DE INGESTA ============================
// Usado para /health: saber último intento/éxito/error de refresh sin tener que forzarlo.
const ingestState = {
  last_attempt_at: null,     // ISO del último intento (OK o error)
  last_ok_at: null,          // ISO del último refresh exitoso (algún área)
  last_error_at: null,       // ISO del último refresh fallido
  last_error_message: null   // Mensaje técnico del último error
};

// Marcar intento de ingesta
function markIngestAttempt() {
  ingestState.last_attempt_at = new Date().toISOString();
}
// Marcar ingesta OK
function markIngestOk() {
  ingestState.last_ok_at = new Date().toISOString();
  ingestState.last_error_at = null;
  ingestState.last_error_message = null;
}
// Marcar error de ingesta
function markIngestError(e) {
  ingestState.last_error_at = new Date().toISOString();
  ingestState.last_error_message = String(e?.message || e);
}

// Explicación legible de errores (para /health)
function explainError(msg) {
  if (!msg) return null;
  const m = String(msg).toLowerCase();
  if (m.includes('http 503')) return 'AEMET no disponible (503 temporal).';
  if (m.includes('http 404')) return 'Recurso de AEMET no encontrado (404).';
  if (m.includes('http 500')) return 'Fallo interno en AEMET (500).';
  if (m.includes('http 429')) return 'Límite de peticiones superado (429).';
  if (m.includes('abort') || m.includes('timeout')) return 'Tiempo de espera agotado al contactar con AEMET.';
  if (m.includes('fetch failed')) return 'Fallo de red al contactar con AEMET.';
  if (m.includes('sin "datos"') || m.includes("sin 'datos'")) return 'Catálogo de AEMET sin campo "datos".';
  if (m.includes('invalid xml') || m.includes('unexpected') || m.includes('xml')) return 'XML de AEMET inválido o corrupto.';
  if (m.includes('gzip') || m.includes('tar') || m.includes('descompres')) return 'Fichero TAR/XML corrupto o no descomprimible.';
  if (m.includes('falta aemet_api_key')) return 'Configuración: falta la API key de AEMET.';
  if (m.includes('parámetro "area" inválido') || m.includes('parametro "area" invalido')) return 'Parámetro "area" inválido (debe ser 2 dígitos).';
  return 'Error de refresco desde AEMET no clasificado.';
}

// Utilidades de tiempo/caducidad
function nowMs() { return Date.now(); }
function isExpired(entry) {
  if (!entry) return true;
  const ageSec = (nowMs() - entry.fetchedAt) / 1000;
  return ageSec > CACHE_TTL_SECONDS;
}

// ========================= UTILIDADES BÁSICAS HTTP/TAR/XML =====================
function assertZona(z) {
  if (!/^\d{6}$/.test(z || '')) {
    const e = new Error('Parámetro "zona" inválido. Debe ser 6 dígitos (p.ej. 614101).');
    e.status = 400;
    throw e;
  }
}
function isGzip(buf) { return buf.length >= 2 && buf[0] === 0x1f && buf[1] === 0x8b; }

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

// ========================= PARSEO CAP v1.2 (normalizado) ======================
const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  textNodeName: '#text',
  trimValues: true,
});
function asArray(x) { return Array.isArray(x) ? x : x == null ? [] : [x]; }

// Normaliza alertas SIN areas para aligerar lo que devolvemos al público
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
      // ⚠️ NO devolvemos "areas" para aligerar
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
      };
    });
    return { header, info: infoList };
  });
}

// Parser para matching (con areas)
function parseCap_FOR_MATCHING(xmlText) {
  const p2 = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    textNodeName: '#text',
    trimValues: true,
  });
  const root = p2.parse(xmlText);
  const alerts = asArray(root?.alert || root?.['cap:alert']);
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

// ========================= MATCH / FILTROS PARA ZONA ===========================
function fileMatchesZonaByName(fileName, zona) { return fileName.includes(zona); }

// ¿El aviso (con areas) contiene la zona por geocódigo?
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

// Extrae areaDesc relevantes para la zona (para enriquecer salida pública)
function extractAreaDescsForZona(parsedAlertWITH_AREAS, zona) {
  const out = new Set();
  const infos = asArray(parsedAlertWITH_AREAS?.info);
  for (const inf of infos) {
    const areas = asArray(inf?.areas);
    for (const a of areas) {
      const geocodes = asArray(a?.geocodes || a?.geocode);
      let match = false;
      for (const g of geocodes) {
        const val = (g?.value ?? g?.['#text'] ?? '') + '';
        if (val.includes(zona)) { match = true; break; }
      }
      if (match && a?.areaDesc) out.add(String(a.areaDesc));
    }
  }
  return Array.from(out);
}

// Filtros personalizados
function isGenericCCAAFileName(fileName) {
  // AFAZ<AREA>VV... => ficheros agregados de Comunidad Autónoma (no usar por zona)
  return /AFAZ\d{2}VV/i.test(fileName || '');
}
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
function alertHasNonGreenLevel(infos) {
  for (const i of (Array.isArray(infos) ? infos : [])) {
    const lvl = getAemetLevelFromInfo(i);
    if (lvl && lvl !== 'verde') return true;
  }
  for (const i of (Array.isArray(infos) ? infos : [])) {
    const sev = String(i?.severity || '').toLowerCase();
    if (sev === 'moderate' || sev === 'severe' || sev === 'extreme') return true;
  }
  return false;
}
function alertLooksGenericCCAA(infos) {
  for (const i of (Array.isArray(infos) ? infos : [])) {
    const hl = String(i?.headline || '').toLowerCase();
    if (hl.includes('ccaa')) return true;
  }
  return false;
}

// ========================= REFRESCO DESDE AEMET (por área) ====================
async function refreshArea(area) {
  if (!AEMET_API_KEY) throw new Error('Falta AEMET_API_KEY en el entorno.');
  const urlCatalogo = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${area}?api_key=${encodeURIComponent(AEMET_API_KEY)}`;

  // 1) HATEOAS (datos/metadatos)
  const cat = await tryFetchJSON(urlCatalogo);
  const urlDatos = cat?.datos;
  if (!urlDatos) throw new Error('Respuesta de AEMET sin "datos".');

  // 2) Descarga TAR/XML
  const dataBuf = await tryFetchBuffer(urlDatos);

  // 3) Extraer entradas (TAR o XML suelto)
  let entries = [];
  let isTar = true;
  try {
    entries = await tarEntries(dataBuf);
  } catch {
    isTar = false;
  }

  // 4) Preparación de contexto de query
  const ficheros = [];
  const nowIso = new Date().toISOString();

  // Guardamos, por zona, qué ficheros aportaron avisos tras filtros (para depurar `ficheros`)
  const usedFilesByZona = new Map(); // zona -> Set(fileName)

  // ⚠️ POLÍTICA DE REEMPLAZO: guarda solo el snapshot del paquete actual para la zona
  function upsertZona(zona, nuevosAvisos, fileList, ctxQuery) {
    if (!zona || nuevosAvisos.length === 0) return;

    // Filtrar ficheros realmente usados para esta zona
    const usedSet = usedFilesByZona.get(zona) || new Set();
    const ficherosFiltrados = fileList.filter(f => usedSet.has(f.name));

    const newPayload = {
      query: { ...ctxQuery, zona },
      ficheros: ficherosFiltrados,
      avisos: nuevosAvisos
    };

    // REEMPLAZO directo: no se fusionan avisos antiguos
    cacheZona.set(zona, { payload: newPayload, fetchedAt: nowMs(), stale: false });
  }

  const baseQuery = {
    area,
    url_catalogo: urlCatalogo,
    url_datos: urlDatos,
    url_metadatos: cat?.metadatos || null,
    last_success_at: nowIso
  };

  if (isTar) {
    // Guardar listado de ficheros para trazabilidad
    for (const ent of entries) {
      ficheros.push({ name: ent.name, size: ent.size, sha1: ent.sha1, matched_by: null });
    }

    const areaCode = String(area).padStart(2, '0');

    // Procesar cada XML del TAR
    for (const e of entries) {
      if (!e.name.toLowerCase().endsWith('.xml')) continue;

      // Omitir ficheros generales CCAA (VV)
      if (isGenericCCAAFileName(e.name)) continue;

      const fileName = e.name;
      const xml = decodeToString(e.buffer);

      // Parseo doble (con areas para matching, sin areas para salida)
      const parsedWithAreas = parseCap_FOR_MATCHING(xml);
      const parsedWithoutAreas = parseCapXmlWithoutAreas(xml);

      // Detectar zonas candidatas
      const posiblesZonas = new Set();

      // 1) Por nombre de fichero AFAZ(\d{6}) que empiece por área
      const m = fileName.match(/AFAZ(\d{6})/i);
      if (m && m[1].startsWith(areaCode)) posiblesZonas.add(m[1]);

      // 2) Por geocódigos dentro del XML (que empiecen por el área)
      for (const pa of parsedWithAreas) {
        for (const inf of asArray(pa.info)) {
          for (const areaObj of asArray(inf.areas || [])) {
            for (const g of asArray(areaObj.geocodes || areaObj.geocode || [])) {
              const val = g?.value ?? g?.['#text'] ?? '';
              const matches = String(val).match(/\b\d{6}\b/g) || [];
              for (const z of matches) {
                if (String(z).startsWith(areaCode)) posiblesZonas.add(String(z));
              }
            }
          }
        }
      }

      // Evaluar avisos por zona candidata
      for (const zona of posiblesZonas) {
        const avisos = [];
        const matchedByName = fileMatchesZonaByName(fileName, zona);

        for (let i = 0; i < parsedWithoutAreas.length; i++) {
          const aSinAreas = parsedWithoutAreas[i];
          const aConAreas = parsedWithAreas[i];

          const matchedGeo = aConAreas ? alertHasZonaByGeocode_WITH_AREAS(aConAreas, zona) : false;
          if (!(matchedByName || matchedGeo)) continue;

          if (!alertHasNonGreenLevel(aSinAreas?.info)) continue;      // Excluir nivel VERDE
          if (alertLooksGenericCCAA(aSinAreas?.info)) continue;       // Excluir genéricos CCAA

          const areaDescs = extractAreaDescsForZona(aConAreas, zona); // Enriquecer con areaDesc
          avisos.push({ file: fileName, ...aSinAreas, areaDescs });

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
    const parsedWithAreas = parseCap_FOR_MATCHING(xml);
    const parsedWithoutAreas = parseCapXmlWithoutAreas(xml);

    const fileName = 'datos.xml';
    const ficherosXml = [{ name: fileName, size: xml.length, sha1: null, matched_by: 'geocode' }];

    const areaCode = String(area).padStart(2, '0');
    const posiblesZonas = new Set();

    for (const pa of parsedWithAreas) {
      for (const inf of asArray(pa.info)) {
        for (const areaObj of asArray(inf.areas || [])) {
          for (const g of asArray(areaObj.geocodes || areaObj.geocode || [])) {
            const val = g?.value ?? g?.['#text'] ?? '';
            const matches = String(val).match(/\b\d{6}\b/g) || [];
            for (const z of matches) {
              if (String(z).startsWith(areaCode)) posiblesZonas.add(String(z));
            }
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
        if (!alertHasNonGreenLevel(aSinAreas?.info)) continue;
        if (alertLooksGenericCCAA(aSinAreas?.info)) continue;

        const areaDescs = extractAreaDescsForZona(aConAreas, zona);
        avisos.push({ file: fileName, ...aSinAreas, areaDescs });

        if (!usedFilesByZona.has(zona)) usedFilesByZona.set(zona, new Set());
        usedFilesByZona.get(zona).add(fileName);
      }
      if (avisos.length > 0) {
        upsertZona(zona, avisos, ficherosXml, baseQuery);
      }
    }
  }

  return { area, filesCount: isTar ? (entries?.length || 0) : 1 };
}

// ========================= AUTH PARA ENDPOINTS ADMIN ===========================
function requireCronToken(req, res, next) {
  const tok = req.headers['x-cron-token'];
  if (!CRON_TOKEN || tok !== CRON_TOKEN) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

// ========================= ENDPOINTS PÚBLICOS ==================================
// Raíz simple
app.get('/', (_, res) => {
  res.type('text/plain').send('AEMET avisos por zona – API de caché (cron/admin + consulta)');
});

// /health enriquecido con estado de último refresh
app.get('/health', (_, res) => {
  const lastOk = ingestState.last_ok_at ? new Date(ingestState.last_ok_at) : null;
  const lastErr = ingestState.last_error_at ? new Date(ingestState.last_error_at) : null;

  // Consideramos OK si hay un OK y no hay error posterior
  const last_refresh_ok = !!lastOk && (!lastErr || lastOk >= lastErr);

  // Último intento conocido
  const last_refresh_at =
    ingestState.last_attempt_at ||
    ingestState.last_ok_at ||
    ingestState.last_error_at ||
    null;

  // Solo mostramos error si es el evento más reciente (o si nunca hubo OK)
  const showError =
    !!ingestState.last_error_message && (!lastOk || (lastErr && lastErr >= lastOk));

  const last_refresh_error = showError ? ingestState.last_error_message : null;
  const last_refresh_error_explained = showError ? explainError(ingestState.last_error_message) : null;

  const sample = Array.from(cacheZona.keys()).slice(0, 5);
  res.json({
    ok: true,
    zones_cached: cacheZona.size,
    sample_zones: sample,
    ttl_seconds: CACHE_TTL_SECONDS,
    last_refresh_at,
    last_refresh_ok,
    last_refresh_error,
    last_refresh_error_explained
  });
});

// Consulta pública por zona (no llama a AEMET)
app.get('/avisos', async (req, res) => {
  try {
    const zona = String(req.query.zona || '').trim();
    assertZona(zona);

    const entry = cacheZona.get(zona);
    if (!entry) {
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

// Estado agregado por áreas (derivado solo de lo que hay en cacheZona)
app.get('/areas/status', (req, res) => {
  try {
    const areaFilter = String(req.query.area || '').trim();
    const includeEmpty = ['1', 'true', 'yes'].includes(String(req.query.include_empty || '').toLowerCase());

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

      const lsStr = entry?.payload?.query?.last_success_at || null;
      const ls = lsStr ? new Date(lsStr) : null;
      if (ls) {
        rec.last_success_at_latest = !rec.last_success_at_latest || ls > rec.last_success_at_latest ? ls : rec.last_success_at_latest;
        rec.last_success_at_earliest = !rec.last_success_at_earliest || ls < rec.last_success_at_earliest ? ls : rec.last_success_at_earliest;
      }

      const fa = new Date(entry.fetchedAt);
      rec.fetched_at_latest = !rec.fetched_at_latest || fa > rec.fetched_at_latest ? fa : rec.fetched_at_latest;
      rec.fetched_at_earliest = !rec.fetched_at_earliest || fa < rec.fetched_at_earliest ? fa : rec.fetched_at_earliest;

      rec.expired_any = rec.expired_any || expired;
      rec.expired_all = rec.expired_all && expired;

      byArea.set(area, rec);
    }

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
            expired_any: null,
            expired_all: null
          });
        }
      }
    }

    const out = Array.from(byArea.values()).map(r => ({
      area: r.area,
      zones_count: r.zones_count,
      sample_zones: Array.from(r.zones).slice(0, 5),
      ttl_seconds: CACHE_TTL_SECONDS,
      last_success_at_latest: r.last_success_at_latest ? r.last_success_at_latest.toISOString() : null,
      last_success_at_earliest: r.last_success_at_earliest ? r.last_success_at_earliest.toISOString() : null,
      fetched_at_latest: r.fetched_at_latest ? r.fetched_at_latest.toISOString() : null,
      fetched_at_earliest: r.fetched_at_earliest ? r.fetched_at_earliest.toISOString() : null,
      expired_any: r.expired_any,
      expired_all: r.expired_all
    }));

    out.sort((a, b) => a.area.localeCompare(b.area));
    res.json({ ok: true, areas: out });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

// ========================= ENDPOINTS ADMIN =====================================
// Refrescar un área (POST /admin/refresh?area=NN)
app.post('/admin/refresh', requireCronToken, async (req, res) => {
  try {
    const area = String(req.query.area || '').trim();
    if (!/^\d{2}$/.test(area)) {
      const e = new Error('Parámetro "area" inválido. Debe ser 2 dígitos (p.ej. 61).');
      e.status = 400;
      throw e;
    }

    // Marcar intento
    markIngestAttempt();

    const r = await refreshArea(area);

    // Marcar éxito global
    markIngestOk();

    res.json({ ok: true, ...r });
  } catch (err) {
    // Marcar error global con mensaje técnico
    markIngestError(err);
    res.status(err.status || 500).json({ ok: false, error: String(err.message || err) });
  }
});

// Refrescar todas las áreas configuradas (POST /admin/refresh-all)
app.post('/admin/refresh-all', requireCronToken, async (req, res) => {
  try {
    const areas = (req.body?.areas && Array.isArray(req.body.areas) ? req.body.areas : AREAS);
    if (!areas || areas.length === 0) {
      return res.status(400).json({ ok: false, error: 'No hay áreas definidas. Usa body {"areas":[..]} o variable AREAS.' });
    }

    // Marcar intento global al inicio
    markIngestAttempt();

    const results = [];
    let anyError = false;

    for (const a of areas) {
      const a2 = String(a).padStart(2, '0');
      try {
        const r = await refreshArea(a2);
        results.push({ area: a2, ok: true, filesCount: r.filesCount });
        await sleep(250); // pequeño respiro para no saturar AEMET
      } catch (e) {
        anyError = true;
        results.push({ area: a2, ok: false, error: String(e.message || e) });
      }
    }

    if (anyError) {
      const lastErrItem = [...results].reverse().find(r => r.ok === false);
      markIngestError({ message: `refresh-all: ${lastErrItem?.area} → ${lastErrItem?.error || 'error'}` });
    } else {
      markIngestOk();
    }

    res.json({ ok: true, results });
  } catch (err) {
    markIngestError(err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

// ========================= ARRANQUE ============================================
app.listen(PORT, () => {
  console.log(`AEMET avisos por zona – caché escuchando en :${PORT}`);
});
