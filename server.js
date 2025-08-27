// server.js
// Microservicio AEMET – caché por zona con refresco vía cron (admin) y lectura pública sin llamadas a AEMET
// --------------------------------------------------------------------------------------------------------
// Versión: política de REEMPLAZO por zona (no acumula avisos antiguos entre paquetes) PERO
// agrega todos los avisos de una misma zona dentro del MISMO paquete (mismo refresh).
// Cambios clave respecto a la versión anterior:
//   • Estado global de ingesta (ingestState) para enriquecer /health con último intento/OK/error.
//   • /health devuelve: last_refresh_at, last_refresh_ok, last_refresh_error, last_refresh_error_explained.
//   • Endpoints admin instrumentados para actualizar ingestState sin cambiar su contrato.
//   • upsertZona ahora ACUMULA en un pendingByZona durante el refresh; al final hace un único set() por zona.
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

// ========================= ESTADO DE INGESTA (para /health) ====================
const ingestState = {
  last_attempt_at: null,
  last_ok_at: null,
  last_error_at: null,
  last_error_message: null
};

// Marcar intento de ingesta
function markIngestAttempt() {
  ingestState.last_attempt_at = new Date().toISOString();
}
// Marcar éxito de ingesta
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
  if (m.includes('gzip') || m.includes('tar') || m.includes('des...mpres')) return 'Fichero TAR/XML corrupto o no descomprimible.';
  if (m.includes('falta aemet_api_key')) return 'Configuración: falta la API key de AEMET.';
  if (m.includes('parámetro "area" inválido') || m.includes('par...ido')) return 'Parámetro "area" inválido (debe ser 2 dígitos).';
  return 'Error de refresco desde AEMET no clasificado.';
}

// Utilidades de tiempo/caducidad
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
function sha1(buf) {
  const h = crypto.createHash('sha1');
  h.update(buf);
  return h.digest('hex');
}

async function gunzipIfNeeded(buf) {
  // Detectar encabezado gzip
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
        out.push({
          name: header.name,
          size: header.size,
          sha1: sha1(buffer),
          buffer
        });
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

// ================== PARSEO CAP v1.2 (normalizado) ======================
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
      // Omitimos areas aquí para aligerar
    }));
    return { header, info: infos };
  });
}

// Parseo con areas (para matching por geocódigo)
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
        return {
          areaDesc: a?.areaDesc ?? null,
          geocodes
        };
      });
      return { ...inf, areas };
    });
    return { info: infos };
  });
}

// Reglas de filtrado “verde” y de avisos genéricos
function alertHasNonGreenLevel(infos) {
  const arr = asArray(infos);
  for (const inf of arr) {
    const sev = (inf?.severity || '').toLowerCase();
    const urg = (inf?.urgency || '').toLowerCase();
    const cer = (inf?.certainty || '').toLowerCase();
    // Aceptamos si hay algún nivel no "minor"/"unknown"
    if (sev && sev !== 'minor' && sev !== 'unknown') return true;
    if (urg && urg !== 'unknown') return true; // si urg no es desconocida, consideramos
    if (cer && cer !== 'unknown') return true;
  }
  return false;
}

function alertLooksGenericCCAA(infos) {
  const arr = asArray(infos);
  for (const inf of arr) {
    const ev = (inf?.event || '').toLowerCase();
    const headline = (inf?.headline || '').toLowerCase();
    // Heurística simple: si menciona comunidad sin concretar zonas, suele existir otra alerta más específica
    if (headline.includes('comunidad') || ev.includes('comunidad')) return true;
  }
  return false;
}

// Coincidencia por geocódigo de zona (6 dígitos) – usando el parseo CON AREAS
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
  // Algunos ficheros con VV genéricos de comunidad se descartan:
  // p.ej. AFA?...VV...? (si AFAZ612903... entonces sí interesa; si AFAZ61VV... genérico, lo descartamos)
  return /AFAZ\d{2}VV/i.test(fileName);
}
function fileMatchesZonaByName(fileName, zona) {
  // Por nombre: AFAZ612903....
  const m = fileName.match(/AFAZ(\d{6})/i);
  return !!(m && m[1] === zona);
}

// ========================= REFRESCO DE ÁREA ====================================
async function refreshArea(area) {
  if (!AEMET_API_KEY) throw new Error('Falta AEMET_API_KEY en el entorno.');
  const urlCatalogo = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoselaborados/area/${area}?api_key=${encodeURIComponent(AEMET_API_KEY)}`;

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
    const maybeGz = await gunzipIfNeeded(dataBuf);
    // ¿Es TAR?
    try {
      entries = await extractTarEntries(maybeGz);
      isTar = true;
    } catch {
      // No es TAR, asumimos XML suelto
      entries = [{ name: 'single.xml', size: maybeGz.length, sha1: sha1(maybeGz), buffer: maybeGz }];
      isTar = false;
    }
  } catch (e) {
    throw new Error(`No se pudo descomprimir/explorar el fichero de datos: ${String(e?.message || e)}`);
  }

  const nowIso = new Date().toISOString();
  const ficheros = []; // trazabilidad para la respuesta
  const usedFilesByZona = new Map(); // zona -> Set(fileName)
  // Acumulador local de avisos por zona durante este refresh
  const pendingByZona = new Map(); // zona -> { avisos: [], usedFiles: Set<string> }

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

      // Descubrir posibles zonas candidatas:
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

          const matched = matchedByName
            ? true
            : (aConAreas ? alertHasZonaByGeocode_WITH_AREAS(aConAreas, zona) : false);

          if (!matched) continue;
          if (!alertHasNonGreenLevel(aSinAreas?.info)) continue;
          if (alertLooksGenericCCAA(aSinAreas?.info)) continue;

          const areaDescs = extractAreaDescsForZona(aConAreas, zona);
          avisos.push({ file: fileName, ...aSinAreas, areaDescs });

          if (!usedFilesByZona.has(zona)) usedFilesByZona.set(zona, new Set());
          usedFilesByZona.get(zona).add(fileName);
        }

        // ACUMULACIÓN por zona (no escribimos en caché aún)
        if (avisos.length > 0) {
          upsertZona(zona, avisos, ficheros, baseQuery);
        }
      }
    }
  } else {
    // XML suelto (raro, pero soportado)
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
        upsertZona(zona, avisos, [{ name: fileName }], baseQuery);
      }
    }
  }

  // === Commit final: escribir en caché por zona con todos los avisos agregados ===
  for (const [zona, acc] of pendingByZona.entries()) {
    const ficherosFiltrados = ficheros.filter(f => acc.usedFiles.has(f.name));
    const newPayload = {
      query: { ...baseQuery, zona },
      ficheros: ficherosFiltrados,
      avisos: acc.avisos
    };
    // Política de REEMPLAZO respecto al snapshot anterior en caché
    cacheZona.set(zona, { payload: newPayload, fetchedAt: nowMs(), stale: false });
  }

  return { area, filesCount: isTar ? (entries?.length || 0) : 1 };

  // --- función local: acumula en pendingByZona en lugar de escribir directamente ---
  function upsertZona(zona, nuevosAvisos, fileList, ctxQuery) {
    if (!zona || nuevosAvisos.length === 0) return;

    // Inicializa acumulador para la zona
    if (!pendingByZona.has(zona)) {
      pendingByZona.set(zona, { avisos: [], usedFiles: new Set() });
    }
    const acc = pendingByZona.get(zona);

    // Agrega avisos de este fichero al acumulado de la zona
    for (const av of nuevosAvisos) acc.avisos.push(av);

    // Marca ficheros realmente usados para la zona (los pondremos al final)
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
// Salud del servicio
app.get('/health', (req, res) => {
  const last_refresh_at = ingestState.last_attempt_at;
  const last_refresh_ok = ingestState.last_ok_at;
  const last_refresh_error = ingestState.last_error_at;
  const last_refresh_error_explained = explainError( ingestState.last_error_message );

  res.json({
    ok: true,
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
    return res.status(err?.status || 500).json({ error: String(err?.message || err) });
  }
});

// Estado de todas las zonas en caché (depuración)
app.get('/debug/cache', (req, res) => {
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

// Resumen por áreas configuradas (útil para ver caducidades)
app.get('/debug/areas', (req, res) => {
  try {
    const areas = AREAS.length ? AREAS : [];
    const out = [];

    for (const area of areas) {
      const rec = {
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

      for (const [zona, entry] of cacheZona.entries()) {
        if (!String(zona).startsWith(String(area).padStart(2, '0'))) continue;

        rec.zones.add(zona);
        rec.zones_count = rec.zones.size;

        const lsStr = entry?.payload?.query?.last_success_at || null;
        const ls = lsStr ? new Date(lsStr) : null;
        if (ls) {
          if (!rec.last_success_at_latest || ls > new Date(rec.last_success_at_latest)) {
            rec.last_success_at_latest = ls;
          }
          if (!rec.last_success_at_earliest || ls < new Date(rec.last_success_at_earliest)) {
            rec.last_success_at_earliest = ls;
          }
        }

        const fetched = new Date(entry.fetchedAt);
        if (!rec.fetched_at_latest || fetched > new Date(rec.fetched_at_latest)) {
          rec.fetched_at_latest = fetched;
        }
        if (!rec.fetched_at_earliest || fetched < new Date(rec.fetched_at_earliest)) {
          rec.fetched_at_earliest = fetched;
        }

        const exp = isExpired(entry);
        rec.expired_any = rec.expired_any || exp;
        rec.expired_all = rec.expired_all && exp;
      }

      out.push({
        area: rec.area,
        zones_count: rec.zones_count,
        last_success_at_latest: rec.last_success_at_latest ? rec.last_success_at_latest.toISOString() : null,
        last_success_at_earliest: rec.last_success_at_earliest ? rec.last_success_at_earliest.toISOString() : null,
        fetched_at_latest: rec.fetched_at_latest ? rec.fetched_at_latest.toISOString() : null,
        fetched_at_earliest: rec.fetched_at_earliest ? rec.fetched_at_earliest.toISOString() : null,
        expired_any: rec.expired_any,
        expired_all: rec.expired_all
      });
    }

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

    res.json({ ok: true, refreshed: r });
  } catch (err) {
    markIngestError(err);
    res.status(err?.status || 500).json({ ok: false, error: String(err.message || err) });
  }
});

// Refrescar todas las áreas configuradas (POST /admin/refresh/all)
app.post('/admin/refresh/all', requireCronToken, async (req, res) => {
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

    // Si alguna falló, lo marcamos como error global; si todas OK, marcamos OK
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
