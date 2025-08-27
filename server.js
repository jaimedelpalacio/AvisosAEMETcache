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
async function tryFetchJSON(url, opts = {}) {
  const r = await fetch(url, { headers: { 'user-agent': UA }, ...opts });
  if (!r.ok) throw new Error(`HTTP ${r.status} al pedir ${url}`);
  return r.json();
}
async function tryFetchBuffer(url, opts = {}) {
  const r = await fetch(url, { headers: { 'user-agent': UA }, ...opts });
  if (!r.ok) throw new Error(`HTTP ${r.status} al pedir ${url}`);
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
      source: alert?.source ?? null,
      scope: alert?.scope ?? null
    };
    const infos = asArray(alert?.info).map((inf) => ({
      language: inf?.language ?? null,
      category: asArray(inf?.category).filter(Boolean),
      event: inf?.event ?? null,
      urgency: inf?.urgency ?? null,
      severity: inf?.severity ?? null,
      certainty: inf?.certainty ?? null,
      effective: inf?.effective ?? null,
      onset: inf?.onset ?? null,
      expires: inf?.expires ?? null,
      headline: inf?.headline ?? null,
      description: inf?.description ?? null,
      instruction: inf?.instruction ?? null,
    }));
    return { header, info: infos };
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
  const arr = asArray(infos);
  for (const inf of arr) {
    const sev = (inf?.severity || '').toLowerCase();
    const urg = (inf?.urgency || '').toLowerCase();
    const cer = (inf?.certainty || '').toLowerCase();
    if (sev && sev !== 'minor' && sev !== 'unknown') return true;
    if (urg && urg !== 'unknown') return true;
    if (cer && cer !== 'unknown') return true;
  }
  return false;
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
  // ⚠️ Mantenemos tu URL de catálogo tal cual (no se toca nada más aquí salvo el agregado por zona)
  const urlCatalogo = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${area}?api_key=${encodeURIComponent(AEMET_API_KEY)}`;

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
          if (!alertHasNonGreenLevel(aSinAreas?.info)) continue;
          if (alertLooksGenericCCAA(aSinAreas?.info)) continue;

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
        if (!alertHasNonGreenLevel(aSinAreas?.info)) continue;
        if (alertLooksGenericCCAA(aSinAreas?.info)) continue;

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
      avisos: acc.avisos
    };
    cacheZona.set(zona, { payload: newPayload, fetchedAt: nowMs(), stale: false });
  }

  return { area, filesCount: isTar ? (entries?.length || 0) : 1 };

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
  const token = String(req.headers['x-cron-token'] || req.query.cron || '');
  if (!CRON_TOKEN || token !== CRON_TOKEN) {
    const e = new Error('No autorizado (X-Cron-Token incorrecto o ausente).');
    e.status = 401;
    return next(e);
  }
  next();
}

// ========================= ENDPOINTS PÚBLICOS ==================================
app.get('/', (req, res) => res.json({ ok: true, name: 'aemet-avisos-zona-cache' }));

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

app.post('/admin/refresh-all', requireCronToken, async (req, res) => {
  try {
    const areas = AREAS.length ? AREAS : [];
    if (!areas.length) {
      const e = new Error('No hay AREAS configuradas en el entorno.');
      e.status = 400;
      throw e;
    }

    markIngestAttempt();

    const results = [];
    for (const area of areas) {
      try {
        const r = await refreshArea(area);
        results.push({ area, ok: true, refreshed: r });
      } catch (e) {
        results.push({ area, ok: false, error: String(e?.message || e) });
      }
    }

    if (results.some(r => !r.ok)) {
      const errors = results.filter(r => !r.ok).map(r => `area ${r.area}: ${r.error}`).join(' | ');
      markIngestError(new Error(errors));
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

// ========================= VALIDACIONES ========================================
function assertZona(zona) {
  if (!/^\d{6}$/.test(zona)) {
    const e = new Error('Parámetro "zona" inválido. Debe ser 6 dígitos (p.ej. 612903).');
    e.status = 400;
    throw e;
  }
}

