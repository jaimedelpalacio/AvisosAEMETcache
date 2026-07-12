// server.js
// Microservicio AEMET – caché por zona con refresco vía cron (admin) y lectura pública sin llamadas a AEMET
// --------------------------------------------------------------------------------------------------------
// Cambios mínimos: solo se agrega por zona dentro del MISMO refresh para no machacar avisos al procesar
// varios XML del mismo paquete. No se cambian rutas ni contratos.
// --------------------------------------------------------------------------------------------------------

import express from 'express';
import { fetch } from 'undici';
import * as zlib from 'zlib';
import tar from 'tar-stream';
import crypto from 'crypto';
import { XMLParser } from 'fast-xml-parser';

const app = express();
const PORT = process.env.PORT || 3000;
const UA = 'MT-Neo-Avisos-Zona/2.x';
const AEMET_API_KEY = process.env.AEMET_API_KEY || '';
const AEMET_BASE_URL = process.env.AEMET_BASE_URL || 'https://opendata.aemet.es';
const CRON_TOKEN = process.env.RENDER_CRON_TOKEN || '';
const CACHE_TTL_SECONDS = parseInt(process.env.CACHE_TTL_SECONDS || '1200', 10); // 20 min por defecto
const AREAS = (process.env.AREAS || '').split(',').map(s => s.trim()).filter(Boolean); // p.ej. "61,62,63"

app.use(express.json({ limit: '4mb' }));

// ========================= CACHÉ EN MEMORIA (por zona) =========================
// Mapa zona (6 dígitos) -> { payload:{ query, ficheros, avisos }, fetchedAt:number(ms), stale:boolean }
const cacheZona = new Map();

// ========================= ESTADO GLOBAL DE INGESTA (para /health) =============
const ingestState = {
  last_attempt_at: null,
  last_ok_at: null,
  last_error_at: null,
  last_error_message: null
};

function markIngestAttempt() { ingestState.last_attempt_at = new Date().toISOString(); }
function markIngestOk() {
  ingestState.last_ok_at = new Date().toISOString();
  ingestState.last_error_at = null;
  ingestState.last_error_message = null;
}
function markIngestError(e) {
  ingestState.last_error_at = new Date().toISOString();
  ingestState.last_error_message = String(e?.message || e);
}

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
  if (m.includes('gzip') || m.includes('tar') || m.includes('des...mpres')) return 'Fichero TAR/XML corrupto o no descomprimible.';
  if (m.includes('falta aemet_api_key')) return 'Configuración: falta la API key de AEMET.';
  if (m.includes('parámetro "area" inválido')) return 'Parámetro "area" inválido (debe ser 2 dígitos).';
  return 'Error de refresco desde AEMET no clasificado.';
}

function nowMs() { return Date.now(); }
function isExpired(entry) {
  if (!entry) return true;
  const ageSec = (nowMs() - entry.fetchedAt) / 1000;
  return ageSec > CACHE_TTL_SECONDS;
}

// ========================= HTTP HELPERS ========================================
// (1) NUEVO: helper de retardo para el backoff
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/**
 * (1) NUEVO: fetch con reintentos exponenciales + jitter y timeout por intento
 * - retries: nº de reintentos (además del primer intento)
 * - baseDelayMs: backoff base (se multiplica por 2^intento)
 * - timeoutMs: tiempo máximo por intento
 */
async function fetchWithRetry(url, opts = {}, { retries = 4, baseDelayMs = 400, timeoutMs = 10000 } = {}) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(new Error('timeout')), timeoutMs);
    try {
      const res = await fetch(url, {
        ...opts,
        headers: { 'user-agent': UA, ...(opts.headers || {}) },
        signal: ctrl.signal
      });
      clearTimeout(timer);
      if (!res.ok) {
        const e = new Error(`HTTP ${res.status} al pedir ${url}`);
        e.httpStatus = res.status;
        throw e;
      }
      return res; // OK
    } catch (err) {
      clearTimeout(timer);
      // 429 = cuota agotada: reintentar solo agrava el problema, propagamos ya
      if (err?.httpStatus === 429) throw err;
      if (attempt === retries) throw err; // último intento, propaga
      const jitter = Math.floor(Math.random() * 250);
      const delay = baseDelayMs * Math.pow(2, attempt) + jitter;
      await sleep(delay);
      // reintenta
    }
  }
}

// (1) CAMBIO: usar fetchWithRetry en estos helpers
async function tryFetchJSON(url, opts = {}) {
  const r = await fetchWithRetry(url, opts, { retries: 4, baseDelayMs: 400, timeoutMs: 10000 });
  return r.json();
}
async function tryFetchBuffer(url, opts = {}) {
  const r = await fetchWithRetry(url, opts, { retries: 4, baseDelayMs: 400, timeoutMs: 12000 });
  return Buffer.from(await r.arrayBuffer());
}

// ========================= TAR / GZIP ==========================================
function sha1(buf) { const h = crypto.createHash('sha1'); h.update(buf); return h.digest('hex'); }
async function gunzipIfNeeded(buf) {
  const isGz = buf.length >= 2 && buf[0] === 0x1F && buf[1] === 0x8B;
  if (!isGz) return buf;
  return new Promise((resolve, reject) => {
    zlib.gunzip(buf, (err, out) => err ? reject(err) : resolve(out));
  });
}
async function extractTarEntries(tarBuf) {
  const extract = tar.extract();
  const out = [];
  await new Promise((resolve, reject) => {
    extract.on('entry', (header, stream, next) => {
      const chunks = [];
      stream.on('data', (c) => chunks.push(c));
      stream.on('end', () => {
        const buffer = Buffer.concat(chunks);
        out.push({ name: header.name, size: header.size, sha1: sha1(buffer), buffer });
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
function decodeToString(b) { try { return b.toString('utf8'); } catch { return b.toString('latin1'); } }

// ================== PARSEO CAP v1.2 (normalizado) ======================
const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_', textNodeName: '#text', trimValues: true });
function asArray(x) { return Array.isArray(x) ? x : x == null ? [] : [x]; }


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
      references: alert?.references ?? null,
    };
    const infoList = asArray(alert?.info).map((info) => {
      // Mantenemos todos los campos que tu Programa anterior podía leer
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

      return {
        language: info?.language ?? null,
        category,
        event: info?.event ?? null,
        responseType,                 // <-- restituido
        urgency: info?.urgency ?? null,
        severity: info?.severity ?? null,
        certainty: info?.certainty ?? null,
        effective: info?.effective ?? null,
        onset: info?.onset ?? null,
        expires: info?.expires ?? null,
        headline: info?.headline ?? null,
        description: info?.description ?? null,
        instruction: info?.instruction ?? null,
        web: info?.web ?? null,       // <-- restituido
        contact: info?.contact ?? null, // <-- restituido
        parameters,                   // <-- restituido
        eventCode                     // <-- restituido
      };
    });
    return { header, info: infoList };
  });
}





function parseCap_FOR_MATCHING(xmlText) {
  const root = parser.parse(xmlText);
  const alerts = asArray(root?.alert || root?.['cap:alert']);
  return alerts.map((alert) => {
    const infos = asArray(alert?.info).map((inf) => {
      const areas = asArray(inf?.area || inf?.areas).map((a) => {
        const geocodes = asArray(a?.geocode || a?.geocodes).map((g) => ({
          valueName: g?.valueName ?? g?.['valueName'] ?? g?.['@_valueName'] ?? null,
          value: g?.value ?? g?.['#text'] ?? null
        }));
        return { areaDesc: a?.areaDesc ?? null, geocodes };
      });
      return { ...inf, areas };
    });
    return { info: infos };
  });
}

function alertHasNonGreenLevel(infos) {
  // Solo la severidad decide el nivel: Minor/Unknown = verde (fuera).
  // (Antes bastaba con urgency/certainty distintas de unknown, y como todos los
  // avisos traen urgency, el filtro de verdes no filtraba nada en la práctica.)
  const arr = asArray(infos);
  for (const inf of arr) {
    const sev = (inf?.severity || '').toLowerCase();
    if (sev && sev !== 'minor' && sev !== 'unknown') return true;
  }
  return false;
}

// Dedup por identifier (gana el "sent" más reciente) y proceso de msgType=Cancel:
// un Cancel elimina los avisos cuyos identifier aparecen en sus references, y el
// propio mensaje Cancel tampoco se sirve.
function consolidateAvisos(avisos) {
  const cancelled = new Set();
  for (const av of avisos) {
    if (String(av?.header?.msgType || '').toLowerCase() !== 'cancel') continue;
    const refs = String(av?.header?.references || '').trim().split(/\s+/).filter(Boolean);
    for (const r of refs) {
      const parts = r.split(','); // formato CAP: "sender,identifier,sent"
      if (parts.length >= 2 && parts[1]) cancelled.add(parts[1]);
    }
  }

  const byId = new Map();
  const sinId = [];
  for (const av of avisos) {
    const h = av?.header || {};
    if (String(h.msgType || '').toLowerCase() === 'cancel') continue;
    if (h.identifier && cancelled.has(h.identifier)) continue;
    if (!h.identifier) { sinId.push(av); continue; }
    const prev = byId.get(h.identifier);
    if (!prev) { byId.set(h.identifier, av); continue; }
    const tPrev = Date.parse(prev?.header?.sent || '') || 0;
    const tNew = Date.parse(h.sent || '') || 0;
    if (tNew >= tPrev) byId.set(h.identifier, av);
  }
  return [...byId.values(), ...sinId];
}

// ¿Todos los info del aviso tienen expires en el pasado? (sin expires parseable => no caducado)
function avisoCaducado(av, nowTs) {
  const infos = asArray(av?.info);
  if (!infos.length) return false;
  let vistos = 0;
  for (const inf of infos) {
    if (!inf?.expires) return false;
    const t = Date.parse(inf.expires);
    if (isNaN(t)) return false;
    vistos++;
    if (t >= nowTs) return false;
  }
  return vistos > 0;
}
function alertLooksGenericCCAA(infos) {
  const arr = asArray(infos);
  for (const inf of arr) {
    const ev = (inf?.event || '').toLowerCase();
    const headline = (inf?.headline || '').toLowerCase();
    if (headline.includes('comunidad') || ev.includes('comunidad')) return true;
  }
  return false;
}
function alertHasZonaByGeocode_WITH_AREAS(parsedAlertWITH_AREAS, zona) {
  const infos = asArray(parsedAlertWITH_AREAS?.info);
  for (const inf of infos) {
    const areas = asArray(inf?.areas);
    for (const a of areas) {
      const geocodes = asArray(a?.geocodes || a?.geocode);
      for (const g of geocodes) {
        const value = g?.value ?? g?.['#text'] ?? '';
        if (String(value).includes(zona)) return true;
      }
    }
  }
  return false;
}
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
function isGenericCCAAFileName(fileName) { return /AFAZ\d{2}VV/i.test(fileName); }
function fileMatchesZonaByName(fileName, zona) { const m = fileName.match(/AFAZ(\d{6})/i); return !!(m && m[1] === zona); }

// ========================= REFRESCO DE ÁREA ====================================
async function refreshArea(area) {
  if (!AEMET_API_KEY) throw new Error('Falta AEMET_API_KEY en el entorno.');
  const startedMs = nowMs();
  // ⚠️ Mantenemos tu URL de catálogo tal cual (no se toca nada más aquí salvo el agregado por zona)
  const urlCatalogo = `${AEMET_BASE_URL}/opendata/api/avisos_cap/ultimoelaborado/area/${area}?api_key=${encodeURIComponent(AEMET_API_KEY)}`;

  const cat = await tryFetchJSON(urlCatalogo);
  const urlDatos = cat?.datos;
  if (!urlDatos) throw new Error('Respuesta de AEMET sin "datos".');

  const dataBuf = await tryFetchBuffer(urlDatos);

  // Extraemos (TAR o XML suelto)
  let entries = [];
  let isTar = true;
  try {
    const maybeGz = await gunzipIfNeeded(dataBuf);
    try { entries = await extractTarEntries(maybeGz); isTar = true; }
    catch { entries = [{ name: 'single.xml', size: maybeGz.length, sha1: sha1(maybeGz), buffer: maybeGz }]; isTar = false; }
  } catch (e) {
    throw new Error(`No se pudo descomprimir/explorar el fichero de datos: ${String(e?.message || e)}`);
  }

  const nowIso = new Date().toISOString();
  const ficheros = [];                         // trazabilidad para la respuesta
  const usedFilesByZona = new Map();           // zona -> Set(fileName)
  const pendingByZona = new Map();             // ⚠️ NUEVO: acumulador local por zona durante este refresh

  const baseQuery = {
    area,
    url_catalogo: urlCatalogo,
    url_datos: urlDatos,
    url_metadatos: cat?.metadatos || null,
    last_success_at: nowIso
  };

  if (isTar) {
    for (const ent of entries) { ficheros.push({ name: ent.name, size: ent.size, sha1: ent.sha1, matched_by: null }); }
    const areaCode = String(area).padStart(2, '0');

    for (const e of entries) {
      if (!e.name.toLowerCase().endsWith('.xml')) continue;
      if (isGenericCCAAFileName(e.name)) continue;

      const fileName = e.name;
      const xml = decodeToString(e.buffer);
      const parsedWithAreas = parseCap_FOR_MATCHING(xml);
      const parsedWithoutAreas = parseCapXmlWithoutAreas(xml);

      const posiblesZonas = new Set();
      const m = fileName.match(/AFAZ(\d{6})/i);
      if (m && m[1].startsWith(areaCode)) posiblesZonas.add(m[1]);

      for (const pa of parsedWithAreas) {
        for (const inf of asArray(pa.info)) {
          for (const areaObj of asArray(inf.areas || [])) {
            for (const g of asArray(areaObj.geocodes || areaObj.geocode || [])) {
              const val = g?.value ?? g?.['#text'] ?? '';
              const matches = String(val).match(/\b\d{6}\b/g) || [];
              for (const z of matches) if (String(z).startsWith(areaCode)) posiblesZonas.add(String(z));
            }
          }
        }
      }

      for (const zona of posiblesZonas) {
        const avisos = [];
        const matchedByName = fileMatchesZonaByName(fileName, zona);

        for (let i = 0; i < parsedWithoutAreas.length; i++) {
          const aSinAreas = parsedWithoutAreas[i];
          const aConAreas = parsedWithAreas[i];

          const matched = matchedByName || (aConAreas ? alertHasZonaByGeocode_WITH_AREAS(aConAreas, zona) : false);
          if (!matched) continue;
          // Los Cancel pasan siempre: consolidateAvisos los usa para anular avisos y luego los descarta
          const esCancel = String(aSinAreas?.header?.msgType || '').toLowerCase() === 'cancel';
          if (!esCancel && !alertHasNonGreenLevel(aSinAreas?.info)) continue;
          if (!esCancel && alertLooksGenericCCAA(aSinAreas?.info)) continue;

          const areaDescs = extractAreaDescsForZona(aConAreas, zona);
          avisos.push({ file: fileName, ...aSinAreas, areaDescs });

          if (!usedFilesByZona.has(zona)) usedFilesByZona.set(zona, new Set());
          usedFilesByZona.get(zona).add(fileName);
        }

        // ================== CAMBIO MINIMO: ACUMULAR, NO ESCRIBIR AÚN ==================
        if (avisos.length > 0) upsertZona(zona, avisos, ficheros, baseQuery);
      }
    }
  } else {
    const fileName = entries[0]?.name || 'single.xml';
    const xml = decodeToString(entries[0].buffer);
    const parsedWithAreas = parseCap_FOR_MATCHING(xml);
    const parsedWithoutAreas = parseCapXmlWithoutAreas(xml);
    const areaCode = String(area).padStart(2, '0');
    const posiblesZonas = new Set();

    const m = fileName.match(/AFAZ(\d{6})/i);
    if (m && m[1].startsWith(areaCode)) posiblesZonas.add(m[1]);

    for (const pa of parsedWithAreas) {
      for (const inf of asArray(pa.info)) {
        for (const areaObj of asArray(inf.areas || [])) {
          for (const g of asArray(areaObj.geocodes || areaObj.geocode || [])) {
            const val = g?.value ?? g?.['#text'] ?? '';
            const matches = String(val).match(/\b\d{6}\b/g) || [];
            for (const z of matches) if (String(z).startsWith(areaCode)) posiblesZonas.add(String(z));
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
        const esCancel = String(aSinAreas?.header?.msgType || '').toLowerCase() === 'cancel';
        if (!esCancel && !alertHasNonGreenLevel(aSinAreas?.info)) continue;
        if (!esCancel && alertLooksGenericCCAA(aSinAreas?.info)) continue;

        const areaDescs = extractAreaDescsForZona(aConAreas, zona);
        avisos.push({ file: fileName, ...aSinAreas, areaDescs });

        if (!usedFilesByZona.has(zona)) usedFilesByZona.set(zona, new Set());
        usedFilesByZona.get(zona).add(fileName);
      }
      if (avisos.length > 0) upsertZona(zona, avisos, [{ name: fileName }], baseQuery);
    }
  }

  // ================== CAMBIO MINIMO: COMMIT FINAL A CACHÉ POR ZONA ==================
  for (const [zona, acc] of pendingByZona.entries()) {
    const ficherosFiltrados = acc.usedFiles.size
      ? acc.ficheros.filter(f => acc.usedFiles.has(f.name))
      : acc.ficheros;

    const newPayload = {
      query: { ...baseQuery, zona },
      ficheros: ficherosFiltrados,
      avisos: consolidateAvisos(acc.avisos)
    };
    cacheZona.set(zona, { payload: newPayload, fetchedAt: nowMs(), stale: false });
  }

  // ================== PURGA: zonas del área sin avisos en este snapshot ==================
  // Un refresh con éxito es la foto completa del área: las zonas cacheadas de este área
  // que no aparecen ahora pasan a servir avisos:[] (200 con lista vacía, no 503) en vez
  // de quedarse congeladas con avisos antiguos.
  {
    const areaCode = String(area).padStart(2, '0');
    for (const zona of cacheZona.keys()) {
      if (!zona.startsWith(areaCode) || pendingByZona.has(zona)) continue;
      cacheZona.set(zona, {
        payload: { query: { ...baseQuery, zona }, ficheros: [], avisos: [] },
        fetchedAt: nowMs(),
        stale: false
      });
    }
  }

  return { area, filesCount: isTar ? (entries?.length || 0) : 1, zones: pendingByZona.size, ms: nowMs() - startedMs };

  // ================== upsertZona: ACUMULA EN pendingByZona ==================
  function upsertZona(zona, nuevosAvisos, fileList, ctxQuery) {
    if (!zona || !nuevosAvisos?.length) return;

    if (!pendingByZona.has(zona)) {
      pendingByZona.set(zona, {
        avisos: [],
        ficheros: fileList.slice(),     // guardamos referencia del catálogo de ficheros
        usedFiles: new Set()
      });
    }
    const acc = pendingByZona.get(zona);

    // Agrega avisos de este fichero
    for (const av of nuevosAvisos) acc.avisos.push(av);

    // Marca ficheros usados realmente por la zona (para trazabilidad)
    const usedSet = usedFilesByZona.get(zona) || new Set();
    for (const f of fileList) {
      if (usedSet.has(f.name)) acc.usedFiles.add(f.name);
    }
  }
}

// ========================= AUTH PARA ENDPOINTS ADMIN ===========================
function requireCronToken(req, res, next) {
  // Solo por cabecera (el token en query string acaba en logs de peticiones)
  // y comparación en tiempo constante.
  const token = Buffer.from(String(req.headers['x-cron-token'] || ''));
  const expected = Buffer.from(CRON_TOKEN);
  const ok = CRON_TOKEN.length > 0
    && token.length === expected.length
    && crypto.timingSafeEqual(token, expected);
  if (!ok) {
    const e = new Error('No autorizado (X-Cron-Token incorrecto o ausente).');
    e.status = 401;
    return next(e);
  }
  next();
}

// ========================= ENDPOINTS PÚBLICOS ==================================
app.get('/', (req, res) => res.json({ ok: true, name: 'aemet-avisos-zona-cache' }));

// Evita el ruido de 404 de los crawlers y pide no ser indexado
app.get('/robots.txt', (req, res) => res.type('text/plain').send('User-agent: *\nDisallow: /\n'));

app.get('/health', (req, res) => {
  const last = {
    attempt: ingestState.last_attempt_at,
    ok: ingestState.last_ok_at,
    error: ingestState.last_error_at
  };

  const lastOk = ingestState.last_ok_at ? new Date(ingestState.last_ok_at) : null;
  const lastErr = ingestState.last_error_at ? new Date(ingestState.last_error_at) : null;

  const showError = !!ingestState.last_error_message && (!lastOk || (lastErr && lastErr >= lastOk));
  const last_refresh_error = showError ? ingestState.last_error_message : null;
  const last_refresh_error_explained = showError ? explainError(ingestState.last_error_message) : null;

  const sample = Array.from(cacheZona.keys()).slice(0, 5);
  res.json({
    ok: true,
    zones_cached: cacheZona.size,
    sample_zones: sample,
    ttl_seconds: CACHE_TTL_SECONDS,
    last_refresh_at: last.attempt,
    last_refresh_ok: last.ok,
    last_refresh_error,
    last_refresh_error_explained
  });
});

app.get('/avisos', (req, res) => {
  try {
    const zona = String(req.query.zona || '').trim();
    assertZona(zona);

    const entry = cacheZona.get(zona);
    if (!entry) return res.status(503).json({ error: 'cache_miss', zona });

    const expired = isExpired(entry);
    const payload = {
      ...entry.payload,
      // Los avisos ya caducados (expires en el pasado) no se sirven
      avisos: (entry.payload?.avisos || []).filter(av => !avisoCaducado(av, nowMs())),
      stale: Boolean(entry.stale || expired),
      cache: {
        fetched_at: new Date(entry.fetchedAt).toISOString(),
        ttl_seconds: CACHE_TTL_SECONDS,
        expired
      }
    };
    return res.json(payload);
  } catch (err) {
    return res.status(err?.status || 500).json({ error: String(err?.message || err) });
  }
});

// (Depuración opcional)
app.get('/areas/status', (req, res) => {
  try {
    const out = [];
    for (const [zona, entry] of cacheZona.entries()) {
      out.push({
        zona,
        avisos: entry?.payload?.avisos?.length || 0,
        fetched_at: new Date(entry.fetchedAt).toISOString(),
        expired: isExpired(entry),
      });
    }
    out.sort((a, b) => a.zona.localeCompare(b.zona));
    res.json({ ok: true, zonas: out });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

// ========================= ENDPOINTS ADMIN (SIN CAMBIOS DE RUTA) ===============
app.post('/admin/refresh', requireCronToken, async (req, res) => {
  try {
    const area = String(req.query.area || '').trim();
    if (!/^\d{2}$/.test(area)) {
      const e = new Error('Parámetro "area" inválido. Debe ser 2 dígitos (p.ej. 61).');
      e.status = 400;
      throw e;
    }
    markIngestAttempt();
    const r = await refreshArea(area);
    markIngestOk();
    res.json({ ok: true, refreshed: r });
  } catch (err) {
    markIngestError(err);
    res.status(err?.status || 500).json({ ok: false, error: String(err.message || err) });
  }
});

// Refresca todas las AREAS en secuencia y actualiza el estado de ingesta.
// Compartido por /admin/refresh-all y la precarga del arranque.
// Si ya hay un barrido en curso (boot + cron + manual pueden coincidir),
// se reutiliza su promesa en vez de lanzar otro y duplicar peticiones a AEMET.
let refreshEnCurso = null;
function refreshAllAreas() {
  if (refreshEnCurso) return refreshEnCurso;
  refreshEnCurso = doRefreshAllAreas().finally(() => { refreshEnCurso = null; });
  return refreshEnCurso;
}

async function doRefreshAllAreas() {
  markIngestAttempt();

  const results = [];
  for (const area of AREAS) {
    try {
      const r = await refreshArea(area);
      results.push({ area, ok: true, refreshed: r });
    } catch (e) {
      results.push({ area, ok: false, error: String(e?.message || e) });
    }
  }

  // (2) CAMBIO: reflejar éxito parcial y también registrar errores
  const anyOk = results.some(r => r.ok);
  const anyFail = results.some(r => !r.ok);

  if (anyOk) {
    // Hubo al menos un área con éxito: actualizamos last_ok_at
    markIngestOk();
  }
  if (anyFail) {
    // Registramos los errores para diagnóstico
    const errors = results.filter(r => !r.ok).map(r => `area ${r.area}: ${r.error}`).join(' | ');
    markIngestError(new Error(errors));
  }

  return results;
}

app.post('/admin/refresh-all', requireCronToken, async (req, res) => {
  try {
    if (!AREAS.length) {
      const e = new Error('No hay AREAS configuradas en el entorno.');
      e.status = 400;
      throw e;
    }

    const results = await refreshAllAreas();
    const anyOk = results.some(r => r.ok);
    // Si TODAS las áreas fallan devolvemos 502: el curl -f del cron marca el run como fallido
    res.status(anyOk ? 200 : 502).json({ ok: anyOk, results });
  } catch (err) {
    markIngestError(err);
    res.status(err?.status || 500).json({ ok: false, error: String(err.message || err) });
  }
});

// ========================= 404 Y ERRORES EN JSON ===============================
// (evita el HTML con stack trace del handler por defecto de Express)
app.use((req, res) => res.status(404).json({ error: 'not_found' }));
app.use((err, req, res, next) => {
  res.status(err?.status || 500).json({ ok: false, error: String(err?.message || err) });
});

// ========================= ARRANQUE ============================================
app.listen(PORT, () => {
  console.log(`AEMET avisos por zona – caché escuchando en :${PORT}`);
  // Precarga: tras un deploy/reinicio la caché en memoria queda vacía; en vez de
  // esperar al siguiente cron (hasta 30 min de 503 cache_miss), refrescamos ya.
  if (AEMET_API_KEY && AREAS.length) {
    refreshAllAreas()
      .then(results => console.log('[BOOT] Precarga de caché:', JSON.stringify(results)))
      .catch(e => console.error('[BOOT] Precarga fallida:', String(e?.message || e)));
  }
});

// ========================= VALIDACIONES ========================================
function assertZona(zona) {
  if (!/^\d{6}$/.test(zona)) {
    const e = new Error('Parámetro "zona" inválido. Debe ser 6 dígitos (p.ej. 612903).');
    e.status = 400;
    throw e;
  }
}
