// server.js
// Node 18+ (fetch nativo vía node-fetch@3 ESM); si usas CommonJS, ajusta los imports.
// Endpoint conservado: GET /avisos?zona=614102
// Lee el TAR de AEMET (área por defecto 61), extrae solo ficheros de la zona,
// parsea CAP XML y devuelve únicamente avisos de nivel amarillo/naranja/rojo,
// sin áreas geográficas (polígonos/geocodes). Mantiene "query", "ficheros", "avisos".

import express from 'express';
import fetch from 'node-fetch';
import { XMLParser } from 'fast-xml-parser';
import tarStream from 'tar-stream';
import zlib from 'zlib';

const app = express();

// === Config ===
const PORT = process.env.PORT || 3000;
// Área por defecto (Andalucía occidental = 61 en tus ejemplos). Cámbialo solo si te lo piden.
const DEFAULT_AREA = process.env.AEMET_AREA || '61';
// API key AEMET (recomiendo ENV). Si no hay, el endpoint seguirá aceptando ?apikey=
const AEMET_API_KEY = process.env.AEMET_API_KEY || '';

// Cache simple en memoria para reducir cargas (TTL en segundos)
const CACHE_TTL = Number(process.env.CACHE_TTL || 60 * 10); // 10 min
const cache = new Map(); // key -> { at:number, data:any }

// === Utilidades ===
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function okLevel(nivel) {
  // admitimos amarillo/naranja/rojo
  if (!nivel) return false;
  const n = nivel.toLowerCase();
  return n.includes('amarillo') || n.includes('naranja') || n.includes('rojo');
}

function filenameMatchesZona(name, zona) {
  // En AEMET, suele venir AFAZ<zona> en el nombre (p.ej., AFAZ614102...)
  // Además, para ficheros "VV61" de ámbito CCAA no habrá match por zona.
  return name.includes(`AFAZ${zona}`);
}

function trimInfo(info) {
  // Del bloque <info> del CAP nos quedamos con campos útiles; NO devolvemos áreas.
  const keep = [
    'language','category','event','responseType','urgency','severity','certainty',
    'effective','onset','expires','headline','description','instruction',
    'web','contact','parameter','eventCode'
  ];
  const out = {};
  for (const k of keep) {
    if (info?.[k] !== undefined) out[k] = info[k];
  }
  // Normaliza nombres CAP a lo que llega con fast-xml-parser (parameter/eventCode plural?)
  // fast-xml-parser mapea etiquetas repetidas a arrays automáticamente si "arrayMode" por defecto detecta multiples.
  // Aseguramos arrays para homogeneidad:
  if (out.category && !Array.isArray(out.category)) out.category = [out.category];
  if (out.responseType && !Array.isArray(out.responseType)) out.responseType = [out.responseType];
  if (out.parameter && !Array.isArray(out.parameter)) out.parameter = [out.parameter];
  if (out.eventCode && !Array.isArray(out.eventCode)) out.eventCode = [out.eventCode];
  return out;
}

function trimHeader(alert) {
  // Cabecera mínima, sin metadatos extra
  const keep = ['identifier','sender','sent','status','msgType','scope'];
  const out = {};
  for (const k of keep) if (alert?.[k] !== undefined) out[k] = alert[k];
  return out;
}

// Extrae avisos (CAP) en español y filtra por nivel (amarillo/naranja/rojo)
function avisosDesdeCAPxml(xmlText) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '',
    // Asegura arrays cuando hay múltiples <info>
    isArray: (name, jpath, isLeafNode, isAttribute) => {
      return ['alert.info', 'info.category','info.responseType','info.parameter','info.eventCode'].includes(jpath);
    }
  });

  const doc = parser.parse(xmlText);
  const alert = doc?.alert;
  if (!alert) return [];

  const header = trimHeader(alert);

  // A veces <info> viene como objeto, normalizamos a array
  const infos = Array.isArray(alert.info) ? alert.info : (alert.info ? [alert.info] : []);

  // Elegimos español si existe; si no, devolvemos todos y ya filtraremos por nivel
  // pero más adelante filtraremos por nivel. Aquí hacemos una salida "estilo" original:
  const infosFiltradas = infos
    .filter(i => i?.language ? i.language.toLowerCase().startsWith('es') : true)
    .map(trimInfo)
    .filter(i => {
      // Determinar nivel desde parameter "AEMET-Meteoalerta nivel"
      const params = i.parameter || [];
      const pNivel = params.find(p => (p?.valueName || p?.value?.valueName)?.toLowerCase?.().includes('nivel'));
      const nivel = pNivel?.value || (typeof pNivel === 'string' ? pNivel : undefined);
      return okLevel(nivel);
    });

  if (infosFiltradas.length === 0) return [];

  // Structure "aviso" como en tus ejemplos: { file, header, info:[...] }
  return [{
    header,
    info: infosFiltradas
  }];
}

// Descarga TAR, recorre entradas, se queda SOLO con ficheros de la zona indicada y devuelve:
// - lista de nombres (ficheros) coincidentes
// - avisos parseados y filtrados por nivel/ámbito (más filtro final por zona)
async function extraerAvisosZonaDesdeTar(tarBuffer, zona) {
  const extract = tarStream.extract();
  const gunzip = zlib.createGunzip();

  const files = [];
  const avisos = [];

  const pending = [];

  extract.on('entry', (header, stream, next) => {
    const name = header.name || '';
    if (filenameMatchesZona(name, zona)) {
      files.push({ name });

      const chunks = [];
      stream.on('data', (c) => chunks.push(c));
      stream.on('end', () => {
        const xml = Buffer.concat(chunks).toString('utf8');
        const parsed = avisosDesdeCAPxml(xml).map(a => ({ file: name, ...a }));
        // Puede devolver 0 o 1 (o más, por seguridad)
        for (const a of parsed) avisos.push(a);
        next();
      });
      stream.on('error', () => next());
    } else {
      // Consumir y seguir
      stream.resume();
      stream.on('end', next);
      stream.on('error', next);
    }
  });

  const done = new Promise((resolve, reject) => {
    extract.on('finish', resolve);
    extract.on('error', reject);
  });

  // Pipe gunzip -> extract
  gunzip.pipe(extract);
  gunzip.end(tarBuffer);

  await done;

  // Filtrado final por ámbito (descartamos avisos "CCAA" y similares)
  // Nota: como hemos filtrado por nombre de fichero AFAZ<zona>, ya son de nivel zona.
  // Aun así, por robustez, descartamos headlines que sean "CCAA".
  const avisosZona = avisos.map(a => {
    const info = (a.info || []).filter(i => (i.headline || '').toLowerCase().includes('ccaa') === false);
    return { ...a, info };
  }).filter(a => a.info.length > 0);

  return { files, avisos: avisosZona };
}

// === Fetch con caché ===
async function getCatalogAndTar(area, apikey) {
  const catUrl = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${encodeURIComponent(area)}?api_key=${encodeURIComponent(apikey)}`;
  const r = await fetch(catUrl, { timeout: 20000 });
  if (!r.ok) throw new Error(`Catálogo AEMET ${r.status}`);
  const cat = await r.json(); // {descripcion, estado, datos, metadatos}
  if (!cat.datos) throw new Error('Catálogo sin URL de datos');

  // Descarga TAR
  const tarResp = await fetch(cat.datos, { timeout: 30000 });
  if (!tarResp.ok) throw new Error(`TAR AEMET ${tarResp.status}`);
  const tarBuffer = Buffer.from(await tarResp.arrayBuffer());

  return { catalogo: cat, tarBuffer };
}

function cacheKey(area) { return `AFAC-${area}`; }

async function getTarWithCache(area, apikey) {
  const key = cacheKey(area);
  const now = Date.now();
  const hit = cache.get(key);
  if (hit && (now - hit.at) / 1000 < CACHE_TTL) {
    return hit.data;
  }
  const fresh = await getCatalogAndTar(area, apikey);
  cache.set(key, { at: now, data: fresh });
  return fresh;
}

// === Endpoint ===
// Ejemplo: /avisos?zona=614102[&area=61][&apikey=...]
app.get('/avisos', async (req, res) => {
  try {
    const zona = (req.query.zona || '').toString().trim();
    if (!zona) {
      return res.status(400).json({ error: 'Falta parámetro zona' });
    }

    // Área fija (no cambiamos nada si no lo pides): por defecto 61, pero permitimos ?area=...
    const area = (req.query.area || DEFAULT_AREA).toString().trim();
    const apikey = (req.query.apikey || AEMET_API_KEY).toString().trim();
    if (!apikey) {
      return res.status(400).json({ error: 'Falta API key de AEMET (env AEMET_API_KEY o query ?apikey=...)' });
    }

    // TAR (con caché por área)
    const { catalogo, tarBuffer } = await getTarWithCache(area, apikey);

    // Extraer solo ficheros de la zona y parsear avisos filtrados
    const { files, avisos } = await extraerAvisosZonaDesdeTar(tarBuffer, zona);

    // Construimos salida tipo “la primera que me pasaste”, pero sin metadatos ni áreas:
    const out = {
      query: {
        area,
        zona,
        url_catalogo: `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${area}?api_key=***`,
        url_datos: catalogo?.datos || null,
        url_metadatos: catalogo?.metadatos || null,
        last_success_at: new Date().toISOString()
      },
      // Solo listamos los ficheros de esa zona (no todos los del TAR)
      ficheros: files,
      // Avisos ya filtrados (solo amarillo/naranja/rojo y sin CCAA)
      avisos
    };

    res.json(out);
  } catch (err) {
    console.error(err);
    res.status(502).json({ error: 'Error obteniendo avisos', detail: String(err?.message || err) });
  }
});

app.get('/', (_req, res) => {
  res.type('text/plain').send('AEMET avisos zona: /avisos?zona=614102');
});

app.listen(PORT, () => {
  console.log(`Servidor escuchando en :${PORT}`);
});
