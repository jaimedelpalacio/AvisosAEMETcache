// server.js
// Microservicio AEMET avisos CAP por zona (sin dependencias innecesarias)
// - Usa Express para exponer /avisos
// - Descarga el TAR.GZ publicado por AEMET para un área (por defecto 61)
// - Cachea en memoria el binario por área durante CACHE_TTL segundos
// - Extrae del TAR solo los ficheros cuya ruta contenga "AFAZ<zona>"
// - Parsea XML CAP y devuelve solo avisos de nivel amarillo/naranja/rojo (sin geometrías)
// - No requiere 'gunzip-maybe' ni 'node-fetch' (Node 18+ ya trae fetch global)

import express from 'express';
import { XMLParser } from 'fast-xml-parser';
import tar from 'tar-stream';
import zlib from 'zlib';
import { Readable, pipeline } from 'stream';
import { promisify } from 'util';

const app = express();
const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;

// Área por defecto que publica AEMET (Península y Baleares)
const DEFAULT_AREA = process.env.AEMET_AREA || '61';

// TTL de la caché en segundos (por área)
const CACHE_TTL = process.env.CACHE_TTL ? Number(process.env.CACHE_TTL) : 600;

// En AEMET puedes pasar la API key por variable de entorno o como query ?apikey=...
const ENV_APIKEY = process.env.AEMET_API_KEY || null;

// Promisificar pipeline para usar async/await si hiciera falta
const streamPipeline = promisify(pipeline);

// Caché en memoria: { [area]: { buffer: Buffer, ts: number, lastUrl: string, metaUrl: string } }
const tarCache = Object.create(null);

// ---- Utilidades -------------------------------------------------------------

/**
 * Descarga el JSON del catálogo "último elaborado" de AEMET para un área.
 * Devuelve { datosUrl, metaUrl } con los enlaces firmados (caducan).
 */
async function getCatalogLinks(area, apikey) {
  const url = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${encodeURIComponent(
    area
  )}?api_key=${encodeURIComponent(apikey)}`;

  const r = await fetch(url);
  if (!r.ok) {
    throw new Error(`AEMET catálogo HTTP ${r.status}`);
  }
  const j = await r.json();
  if (!j || !j.datos) {
    throw new Error(`AEMET catálogo sin 'datos'`);
  }
  return { datosUrl: j.datos, metaUrl: j.metadatos || null, catalogUrl: url };
}

/**
 * Descarga el TAR.GZ de avisos para un área. Usa caché por área.
 * Devuelve { buffer, originUrl, metaUrl, fetchedAt }
 */
async function getAreaTarGz(area, apikey) {
  // Si hay caché válida, devolverla
  const now = Date.now();
  const cached = tarCache[area];
  if (cached && now - cached.ts < CACHE_TTL * 1000 && Buffer.isBuffer(cached.buffer)) {
    return { buffer: cached.buffer, originUrl: cached.lastUrl, metaUrl: cached.metaUrl, fetchedAt: new Date(cached.ts).toISOString() };
  }

  // Obtener enlaces con firma
  const { datosUrl, metaUrl, catalogUrl } = await getCatalogLinks(area, apikey);

  // Descargar binario
  const r = await fetch(datosUrl);
  if (!r.ok) {
    throw new Error(`AEMET datos HTTP ${r.status}`);
  }
  const ab = await r.arrayBuffer();
  const buffer = Buffer.from(ab);

  // Guardar en caché
  tarCache[area] = { buffer, ts: now, lastUrl: datosUrl, metaUrl, catalogUrl };
  return { buffer, originUrl: datosUrl, metaUrl, fetchedAt: new Date(now).toISOString(), catalogUrl };
}

/**
 * Lee un TAR (después de gunzip) y extrae las entradas cuyo nombre contenga pattern.
 * Devuelve lista de objetos { name, xml } (contenido UTF-8).
 */
async function extractMatchingFromTarBuffer(tarBuffer, pattern) {
  return new Promise((resolve, reject) => {
    const extract = tar.extract();
    const out = [];

    extract.on('entry', (header, stream, next) => {
      const name = header?.name || '';
      const chunks = [];
      stream.on('data', (d) => chunks.push(d));
      stream.on('end', () => {
        if (name.includes(pattern)) {
          out.push({ name, xml: Buffer.concat(chunks).toString('utf8') });
        }
        next(); // continuar con la siguiente entrada
      });
      stream.on('error', reject);
      // Importante: consumir el stream
      stream.resume();
    });

    extract.on('finish', () => resolve(out));
    extract.on('error', reject);

    // Alimentar el extractor con el buffer del TAR
    Readable.from(tarBuffer).pipe(extract);
  });
}

/**
 * AEMET publica .tar.gz, así que descomprimimos con zlib y luego pasamos el TAR a tar-stream.
 */
function gunzipBuffer(gzBuffer) {
  return zlib.gunzipSync(gzBuffer);
}

/**
 * Normaliza valores CAP que pueden ser array o escalar.
 */
function asArray(x) {
  if (x == null) return [];
  return Array.isArray(x) ? x : [x];
}

/**
 * Dado un objeto CAP <info>, identificar si es español y si el nivel es amarillo/naranja/rojo.
 * Devuelve { keep: boolean, level: 'yellow'|'orange'|'red'|null }
 */
function checkInfoKeep(info) {
  // Idioma
  const lang = (info?.language || info?.Language || '').toLowerCase();
  const isES = lang.startsWith('es');

  // Algunos CAP de AEMET traen los parámetros awareness (nivel de color) en <parameter>
  // Buscamos valueName típicas y heurísticas de texto
  const params = asArray(info?.parameter);
  let level = null;

  for (const p of params) {
    const key = (p?.valueName || p?.valuename || '').toLowerCase();
    const val = String(p?.value ?? p?.Value ?? '').toLowerCase();

    // Buscamos indicadores comunes
    const hay =
      val.includes('yellow') || val.includes('amarill') ||
      val.includes('orange') || val.includes('naranj') ||
      val.includes('red') || val.includes('rojo');

    if (key.includes('awareness') || key.includes('nivel') || hay) {
      if (val.includes('red') || val.includes('rojo')) level = 'red';
      else if (val.includes('orange') || val.includes('naranj')) level = 'orange';
      else if (val.includes('yellow') || val.includes('amarill')) level = 'yellow';
    }
  }

  // Heurística adicional: a veces el titular ya trae el color
  const headline = String(info?.headline || '').toLowerCase();
  if (!level) {
    if (headline.includes('rojo')) level = 'red';
    else if (headline.includes('naranja')) level = 'orange';
    else if (headline.includes('amarill')) level = 'yellow';
  }

  // Excluir avisos de ámbito "CCAA" (heurística: aparece en headline o areaDesc)
  const areaDescs = asArray(info?.area).map((a) => (a?.areaDesc || a?.areadesc || '')).join(' ').toLowerCase();
  const isCCAA = headline.includes('ccaa') || areaDescs.includes('ccaa');

  const keep = isES && !!level && !isCCAA;
  return { keep, level: level || null };
}

/**
 * Parsea un XML CAP -> objeto reducido { header, info[] (filtrados y en español) }
 */
function parseCapXml(xml) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    // Preservar arrays cuando hay uno o varios nodos
    isArray: (name, jpath, isLeafNode, isAttribute) => {
      return ['alert.info', 'info.parameter', 'info.area', 'resource'].some((p) => jpath.endsWith(p));
    },
  });

  const j = parser.parse(xml);
  const alert = j?.alert || j?.Alert || null;
  if (!alert) return null;

  // Cabezera básica
  const header = {
    identifier: alert.identifier || null,
    sender: alert.sender || null,
    sent: alert.sent || null,
    status: alert.status || null,
    msgType: alert.msgType || null,
    scope: alert.scope || null,
  };

  // Seleccionar y filtrar info
  const infosRaw = asArray(alert.info || alert.Info);
  const infosKeep = [];
  for (const info of infosRaw) {
    const { keep, level } = checkInfoKeep(info);
    if (keep) {
      // Reducir el objeto para que sea más compacto y coherente
      const reduced = {
        language: info.language || null,
        category: info.category || null,
        event: info.event || null,
        headline: info.headline || null,
        description: info.description || null,
        effective: info.effective || null,
        onset: info.onset || null,
        expires: info.expires || null,
        urgency: info.urgency || null,
        severity: info.severity || null,
        certainty: info.certainty || null,
        parameters: asArray(info.parameter).map((p) => ({
          name: p?.valueName || p?.valuename || null,
          value: p?.value || p?.Value || null,
        })),
        areas: asArray(info.area).map((a) => ({
          areaDesc: a?.areaDesc || a?.areadesc || null,
          // OJO: no devolvemos polígonos ni geometrias (petición original)
        })),
        _level: level, // amarillo|naranja|rojo (normalizado)
      };
      infosKeep.push(reduced);
    }
  }

  if (infosKeep.length === 0) return null;
  return { header, info: infosKeep };
}

/**
 * A partir de los ficheros XML extraídos de la zona, parsea y filtra CAP.
 */
function buildAvisosFromFiles(files) {
  const avisos = [];
  for (const f of files) {
    const parsed = parseCapXml(f.xml);
    if (parsed) avisos.push(parsed);
  }
  return avisos;
}

// ---- Rutas HTTP -------------------------------------------------------------

app.get('/', (_req, res) => {
  res.type('text/plain').send('AEMET avisos zona: /avisos?zona=614102[&area=61][&apikey=...]');
});

app.get('/avisos', async (req, res) => {
  try {
    const zona = String(req.query.zona || '').trim();
    const area = String(req.query.area || DEFAULT_AREA).trim();
    const apikey = String(req.query.apikey || ENV_APIKEY || '').trim();

    if (!zona) {
      return res.status(400).json({ error: "Falta parámetro 'zona' (6 dígitos)" });
    }
    if (!/^\d{6}$/.test(zona)) {
      return res.status(400).json({ error: "El parámetro 'zona' debe tener 6 dígitos" });
    }
    if (!apikey) {
      return res.status(400).json({ error: "Falta 'apikey' (o define AEMET_API_KEY en el entorno)" });
    }

    // Descargar TAR.GZ (con caché)
    const { buffer: gz, originUrl, metaUrl, fetchedAt, catalogUrl } = await getAreaTarGz(area, apikey);

    // Descomprimir
    const tarBuf = gunzipBuffer(gz);

    // Filtrar ficheros por patrón AFAZ<zona>
    const pattern = `AFAZ${zona}`;
    const files = await extractMatchingFromTarBuffer(tarBuf, pattern);

    // Parsear CAP y filtrar niveles
    const avisos = buildAvisosFromFiles(files);

    const payload = {
      query: {
        zona,
        area,
        catalog: catalogUrl,
        datos: originUrl,
        metadatos: metaUrl,
        last_success_at: fetchedAt || new Date().toISOString(),
      },
      ficheros: files.map((f) => f.name),
      avisos,
    };

    res.json(payload);
  } catch (err) {
    // Errores de red/parseo externos => 502 Bad Gateway
    res.status(502).json({ error: (err && err.message) || String(err) });
  }
});

// ---- Arranque ---------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`AEMET avisos zona escuchando en http://0.0.0.0:${PORT}`);
});
