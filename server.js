// server.js
// API ligera de avisos AEMET por area y zona, sin metadatos ni áreas geográficas.
// Ejemplo de uso: GET /avisos?area=61&zona=614102&api_key=XXXX

import express from "express";
import { createGunzip } from "zlib";
import gunzipMaybe from "gunzip-maybe";
import tar from "tar-stream";
import { XMLParser } from "fast-xml-parser";

// --- Config básica ---
const app = express();
const PORT = process.env.PORT || 3000;

// Parser XML
const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  trimValues: true,
  allowBooleanAttributes: true,
});

// Util: convierte array a siempre-array
const toArray = (v) => (Array.isArray(v) ? v : v ? [v] : []);

// Extrae “parameters” como objeto clave→valor
function parametersToMap(info) {
  const out = {};
  const params = toArray(info?.parameter);
  for (const p of params) {
    const k = p?.valueName ?? p?.["valueName"];
    const v = p?.value ?? p?.["value"];
    if (k) out[k] = v ?? null;
  }
  return out;
}

// Busca la sección <info> en español si existe; si no, la primera
function pickEsInfo(alert) {
  const infos = toArray(alert?.alert?.info ?? alert?.info);
  if (!infos.length) return null;
  const es = infos.find((i) => (i.language ?? i?.["language"]) === "es-ES");
  return es || infos[0];
}

// Comprueba si el aviso incluye la zona solicitada
function infoMatchesZona(info, zona) {
  // Estructuras posibles: <area><geocode><valueName>AEMET-Meteoalerta zona</valueName><value>614102</value>
  const areas = toArray(info?.area);
  for (const a of areas) {
    const geocodes = toArray(a?.geocode);
    for (const g of geocodes) {
      const name = g?.valueName ?? g?.["valueName"];
      const val = String(g?.value ?? g?.["value"] ?? "");
      if (name && name.includes("AEMET-Meteoalerta zona")) {
        if (val === String(zona)) return true;
      }
    }
  }
  return false;
}

// Aplana a un objeto “ligero” compatible con MT Neo (sin metadatos ni polígonos)
function simplifyAlert(alertJson, fileName) {
  const alert = alertJson?.alert ?? alertJson; // por si el parser mete raíz “alert”
  const header = {
    identifier: alert?.identifier ?? null,
    sender: alert?.sender ?? null,
    sent: alert?.sent ?? null,
    status: alert?.status ?? null,
    msgType: alert?.msgType ?? null,
    scope: alert?.scope ?? null,
  };

  const info = pickEsInfo(alertJson);
  if (!info) return null;

  const params = parametersToMap(info);

  // “Evento/fenómeno” puede venir en <event> y/o en <eventCode><value>AT;Temperaturas máximas</value>
  const eventCodeArr = toArray(info?.eventCode);
  const eventCodeRaw = eventCodeArr.find((e) => (e?.value ?? "").includes(";"))?.value ?? null;
  const fenomeno = eventCodeRaw ? eventCodeRaw.split(";")[0] : null; // ej. "AT"
  const fenomeno_desc = eventCodeRaw ? eventCodeRaw.split(";").slice(1).join(";") : info?.event ?? null;

  // Nivel (verde/amarillo/naranja/rojo) suele ir en parámetro “AEMET-Meteoalerta nivel”
  const nivel = params["AEMET-Meteoalerta nivel"] ?? null;

  // Probabilidad
  const probabilidad = params["AEMET-Meteoalerta probabilidad"] ?? null;

  // Parámetro (ej. TA;Temperatura máxima;44 ºC) si viene
  const parametro = params["AEMET-Meteoalerta parametro"] ?? null;

  // Construimos objeto ligero y legible: sin áreas ni raw_xml
  return {
    file: fileName || null,
    header, // (ligero: sin metadatos extra)
    info: {
      language: info?.language ?? null,
      category: toArray(info?.category).filter(Boolean),
      event: info?.event ?? fenomeno_desc ?? null,
      fenomeno: fenomeno, // ej. “AT”
      nivel,              // ej. “rojo”
      responseType: toArray(info?.responseType).filter(Boolean),
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
      parametro,          // ej. “TA;Temperatura máxima;44 ºC”
      probabilidad,       // ej. “40%-70%”
    },
  };
}

// Descarga JSON del catálogo (para obtener URL del TAR “datos”)
async function fetchCatalog(area, apiKey) {
  const url = `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${encodeURIComponent(
    area
  )}?api_key=${encodeURIComponent(apiKey)}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Catálogo AEMET HTTP ${res.status}`);
  return res.json();
}

// Descarga y procesa el TAR: devuelve lista de avisos simplificados filtrados por zona
async function fetchAvisosFromTar(datosUrl, zona) {
  const res = await fetch(datosUrl);
  if (!res.ok) throw new Error(`Datos TAR HTTP ${res.status}`);

  const extract = tar.extract();
  const avisos = [];

  const stream = res.body.pipe(gunzipMaybe()).pipe(extract);

  await new Promise((resolve, reject) => {
    extract.on("entry", async (header, streamEntry, next) => {
      try {
        const { name } = header || {};
        if (name && name.toLowerCase().endsWith(".xml")) {
          let xml = "";
          streamEntry.on("data", (c) => (xml += c.toString("utf8")));
          streamEntry.on("end", () => {
            try {
              const parsed = xmlParser.parse(xml);
              const esInfo = pickEsInfo(parsed);
              if (esInfo && infoMatchesZona(esInfo, zona)) {
                const simple = simplifyAlert(parsed, name);
                if (simple) avisos.push(simple);
              }
            } catch (e) {
              // si un XML falla, seguimos con el resto
              // console.error("XML parse error:", e);
            }
            next();
          });
        } else {
          // no es xml; saltar
          streamEntry.resume();
          streamEntry.on("end", next);
        }
      } catch (e) {
        next(); // continuar a la siguiente entrada pese al error
      }
    });

    extract.on("finish", resolve);
    extract.on("error", reject);
    stream.on("error", reject);
  });

  // Orden opcional por “onset” (más próximo primero) y luego por “sent”
  avisos.sort((a, b) => {
    const ao = Date.parse(a.info.onset || "") || 0;
    const bo = Date.parse(b.info.onset || "") || 0;
    if (ao !== bo) return ao - bo;
    const as = Date.parse(a.header.sent || "") || 0;
    const bs = Date.parse(b.header.sent || "") || 0;
    return as - bs;
  });

  return avisos;
}

app.get("/avisos", async (req, res) => {
  try {
    const area = req.query.area;
    const zona = req.query.zona;
    const apiKey = req.query.api_key || process.env.AEMET_API_KEY;

    if (!area || !zona || !apiKey) {
      return res.status(400).json({
        error: "Parámetros requeridos: area, zona, api_key",
      });
    }

    // 1) Catálogo -> obtenemos URL de “datos” (TAR)
    const catalog = await fetchCatalog(area, apiKey);
    const url_datos = catalog?.datos || catalog?.["datos"];
    if (!url_datos) throw new Error("Catálogo sin URL de datos");

    // 2) TAR -> filtrar y aplanar avisos por zona
    const avisos = await fetchAvisosFromTar(url_datos, zona);

    // 3) Respuesta LIGERA, compatible con tus programas:
    //    - sin “metadatos”
    //    - sin “areas” ni “polygons/circles”
    //    - múltiples avisos (hoy/mañana/pasado) si existen
    res.json({
      query: {
        area: String(area),
        zona: String(zona),
        url_catalogo: `https://opendata.aemet.es/opendata/api/avisos_cap/ultimoelaborado/area/${area}`,
        url_datos,
        last_success_at: new Date().toISOString(),
      },
      avisos,
      stale: false,
    });
  } catch (err) {
    res.status(500).json({
      error: String(err?.message || err),
    });
  }
});

app.listen(PORT, () => {
  console.log(`Avisos AEMET listo en http://localhost:${PORT}`);
});
