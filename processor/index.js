import "dotenv/config";
import express from "express";
import axios from "axios";
import { createClient } from "@supabase/supabase-js";
import { parse } from "csv-parse";
import { Readable } from "stream";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import xmlrpc from "xmlrpc";


// ---------------- CONFIG ----------------
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const BUCKET = process.env.SUPABASE_BUCKET || "market-csv";

const INTERVAL_SECONDS = Number(process.env.PROCESSOR_INTERVAL_SECONDS || 60);
const WEBHOOK_PORT = Number(process.env.PORT || process.env.WEBHOOK_PORT || 7070);

// URL "publica" que o XML Service vai chamar
const WEBHOOK_PUBLIC_URL =
  process.env.WEBHOOK_PUBLIC_URL || `http://localhost:${WEBHOOK_PORT}`;

//endpoint do xmlrpc
const XMLRPC_URL = process.env.XMLRPC_URL || ""; 


// tempo maximo para esperar callback do XML Service
const WEBHOOK_WAIT_SECONDS = Number(process.env.WEBHOOK_WAIT_SECONDS || 60);

// mapper version
const MAPPER_VERSION = process.env.MAPPER_VERSION || "v1";

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("Faltam SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY no .env");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const app = express();
app.use(express.json({ limit: "10mb" }));


// ---------------- AXIOS CONFIG ----------------
const AXIOS_CONFIG = {
  timeout: 15000,
  headers: {
    "User-Agent": "TP3-IS-Processor/1.0 (educational)",
    Accept: "application/json",
  },
};
// ----------------------------------------------

// ---------------- FOLDERS ----------------
const PROJECT_ROOT = process.cwd();

const ERRORS_DIR = path.join(PROJECT_ROOT, "errors");
const OUTPUT_DIR = path.join(PROJECT_ROOT, "output");


if (!fs.existsSync(ERRORS_DIR)) fs.mkdirSync(ERRORS_DIR, { recursive: true });
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });



function csvEscape(value) {
  const s = String(value ?? "");
  return `"${s.replace(/"/g, '""')}"`;
}

function logError(errorsFile, sourceFile, requestId, country, errorMsg) {
  const line =
    `${csvEscape(new Date().toISOString())},` +
    `${csvEscape(sourceFile)},` +
    `${csvEscape(requestId)},` +
    `${csvEscape(country)},` +
    `${csvEscape(errorMsg)}\n`;
  fs.appendFileSync(errorsFile, line, "utf8");
}


function tsCompact() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return (
    d.getUTCFullYear() +
    pad(d.getUTCMonth() + 1) +
    pad(d.getUTCDate()) +
    "_" +
    pad(d.getUTCHours()) +
    pad(d.getUTCMinutes()) +
    pad(d.getUTCSeconds())
  );
}

function getErrorReason(err) {
  const status = err?.response?.status;
  const url = err?.config?.url;
  const code = err?.code;
  const msg = err?.message;

  if (status) return `HTTP ${status}${url ? ` (${url})` : ""}`;
  if (code) return `${code}${url ? ` (${url})` : ""}`;
  return msg || "Erro desconhecido";
}
// ----------------------------------------

// ---------------- WEBHOOK (ACK do XML Service) ----------------
// Vamos guardar o estado por requestId: { ok: true/false, receivedAt, payload }
const webhookAcks = new Map();

app.post("/webhook/xml-service", (req, res) => {
  // Esperado algo tipo: { requestId: "...", ok: true, message: "...", xmlSaved: true }
  const body = req.body || {};
  const requestId = body.requestId || body.request_id;

  if (requestId) {
    webhookAcks.set(requestId, {
      ok: Boolean(body.ok ?? body.success ?? body.xmlSaved ?? true),
      receivedAt: new Date().toISOString(),
      payload: body,
    });
    console.log("Webhook recebido do XML Service:", body);
  } else {
    console.log("Webhook recebido (sem requestId):", body);
  }

  res.status(200).json({ ok: true });
});

async function waitForWebhookAck(requestId, timeoutSeconds) {
  const deadline = Date.now() + timeoutSeconds * 1000;

  while (Date.now() < deadline) {
    const ack = webhookAcks.get(requestId);
    if (ack) return ack;
    await new Promise((r) => setTimeout(r, 500));
  }

  return null;
}
// ----------------------------------------

// ---------------- HELPERS ----------------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}


const INCOMING_PREFIX = "incoming"; // onde o crawler mete os CSVs

async function listCsvFiles() {
  const { data, error } = await supabase.storage.from(BUCKET).list(INCOMING_PREFIX, {
    limit: 100,
    offset: 0,
    sortBy: { column: "name", order: "asc" }, // FIFO
  });
  if (error) throw error;

  return (data || [])
    .map((f) => `${INCOMING_PREFIX}/${f.name}`)
    .filter((fullPath) => fullPath.endsWith(".csv") && path.basename(fullPath).startsWith("countries_population_"))
    .sort();
}

async function downloadCsvStream(filePath) {
  const { data, error } = await supabase.storage.from(BUCKET).download(filePath);
  if (error) throw error;
  return Readable.fromWeb(data.stream());
}

async function deleteCsvFromBucket(filePath) {
  const { data, error } = await supabase.storage.from(BUCKET).remove([filePath]);
  if (error) throw error;
  return data;
}




async function uploadBufferToBucket(bucketPath, buffer, contentType) {
  const { error } = await supabase.storage.from(BUCKET).upload(bucketPath, buffer, {
    contentType,
    upsert: true, // importante: permite reescrever ficheiros
  });
  if (error) throw error;
}

async function uploadFileToBucket(localPath, bucketPath, contentType) {
  const buffer = fs.readFileSync(localPath);
  await uploadBufferToBucket(bucketPath, buffer, contentType);
}



// ----------------------------------------

// ---------------- ENRICHMENT ----------------
async function enrichWithApis(record) {
  const countryName = (record.country || "").trim();
  if (!countryName || countryName.toLowerCase() === "world") return null;

  // API 1: REST Countries
  let countryData = null;
  try {
    let response;
    try {
      response = await axios.get(
        `https://restcountries.com/v3.1/name/${encodeURIComponent(countryName)}?fullText=true`,
        AXIOS_CONFIG
      );
    } catch {
      response = await axios.get(
        `https://restcountries.com/v3.1/name/${encodeURIComponent(countryName)}`,
        AXIOS_CONFIG
      );
    }
    countryData = Array.isArray(response.data) ? response.data[0] : null;
  } catch (err) {
    // Se falhar REST Countries, nao temos ISO/currency, entao e falha "hard"
    throw new Error(`REST Countries falhou: ${getErrorReason(err)}`);
  }

  if (!countryData) return null;

  const iso2 = countryData.cca2 ?? null;
  const iso3 = countryData.cca3 ?? null;

  const currencyCode = countryData.currencies ? Object.keys(countryData.currencies)[0] : null;

  // API 2: World Bank (ISO3)
  // se falhar -> gdp fica null e registamos erro fora (no caller)
  let gdpUsd = null;
  let gdpYear = null;
  let worldBankError = null;

  if (iso3) {
    try {
      const wb = await axios.get(
        `https://api.worldbank.org/v2/country/${iso3}/indicator/NY.GDP.MKTP.CD?format=json`,
        AXIOS_CONFIG
      );

      const data = wb.data?.[1];
      if (Array.isArray(data)) {
        const valid = data.find((d) => d && d.value !== null);
        if (valid) {
          gdpUsd = valid.value;
          gdpYear = valid.date;
        }
      }
    } catch (err) {
      worldBankError = `World Bank falhou: ${getErrorReason(err)}`;
    }
  }

  return {
    countryName,
    iso2,
    iso3,
    populationTotal: Number(record.population),
    worldPct: Number(record.world_pct),
    currencyCode,
    gdpUsd,
    gdpYear,
    mapper_version: MAPPER_VERSION,
    enriched_at: new Date().toISOString(),
    worldBankError, // opcional (para o caller decidir se loga)
  };
}
// ------------------------------------------

// ---------------- XML SERVICE SEND ----------------
async function sendToXmlService({ requestId, mapperVersion, webhookUrl, csvPath }) {
  if (!XMLRPC_URL) {
    console.log("XMLRPC_URL nao definido. A saltar envio para XML Service.");
    return { skipped: true };
  }

  // enviar o ficheiro como base64 (RPC não “manda ficheiros” nativamente)
  const csvBase64 = fs.readFileSync(csvPath).toString("base64");

  const u = new URL(XMLRPC_URL);

  const client =
    u.protocol === "https:"
      ? xmlrpc.createSecureClient({
          host: u.hostname,
          port: u.port ? Number(u.port) : 443,
          path: u.pathname || "/rpc",
        })
      : xmlrpc.createClient({
          host: u.hostname,
          port: u.port ? Number(u.port) : 80,
          path: u.pathname || "/rpc",
        });


  return await new Promise((resolve, reject) => {
    client.methodCall(
      "xml.ingestCsv",
      [
        {
          requestId,
          webhookUrl,
          mapper_version: mapperVersion,
          filename: path.basename(csvPath),
          csv_base64: csvBase64,
        },
      ],
      (err, value) => {
        if (err) return reject(err);
        resolve(value);
      }
    );
  });
}

// ------------------------------------------

// ---------------- PIPELINE ----------------
async function runOnce() {

  console.log("Processor: a listar CSVs...");
  const files = await listCsvFiles();

  if (files.length === 0) {
    console.log("Processor: nenhum CSV encontrado.");
    return false;
  }


  const target = files[0]; // tipo: incoming/countries_population_....csv
  const requestId = crypto.randomUUID();

  console.log("Processor: a processar (FIFO):", target);
  console.log("Processor: requestId:", requestId);

  const csvStream = await downloadCsvStream(target);

  const parser = csvStream.pipe(
    parse({
      columns: true,
      skip_empty_lines: true,
    })
  );

  // ---- ficheiros locais desta run
  const baseTarget = path.basename(target); // evita slashes no nome
  const outName = `enriched_${tsCompact()}_${baseTarget}`;
  const outPath = path.join(OUTPUT_DIR, outName);

  const errorsRunFile = path.join(ERRORS_DIR, `processing_errors_${requestId}.csv`);
  fs.writeFileSync(errorsRunFile, "timestamp,source_file,request_id,country,error\n", "utf8");

  const out = fs.createWriteStream(outPath, { encoding: "utf8" });
  out.write(
    "countryName,iso2,iso3,populationTotal,worldPct,currencyCode,gdpUsd,gdpYear,mapper_version,enriched_at\n"
  );

  let okCount = 0;
  let failCount = 0;

  for await (const record of parser) {
    const rawCountry = record.country;

    try {
      const enriched = await enrichWithApis(record);

      if (!enriched) {
        failCount++;
        logError(
          errorsRunFile,
          target,
          requestId,
          rawCountry,
          "Falha no enrichment (sem match / country invalido)"
        );
        continue;
      }

      if (enriched.worldBankError) {
        logError(errorsRunFile, target, requestId, rawCountry, enriched.worldBankError);
      }

      okCount++;

      const line =
        `${csvEscape(enriched.countryName)},` +
        `${csvEscape(enriched.iso2)},` +
        `${csvEscape(enriched.iso3)},` +
        `${csvEscape(enriched.populationTotal)},` +
        `${csvEscape(enriched.worldPct)},` +
        `${csvEscape(enriched.currencyCode)},` +
        `${csvEscape(enriched.gdpUsd)},` +
        `${csvEscape(enriched.gdpYear)},` +
        `${csvEscape(enriched.mapper_version)},` +
        `${csvEscape(enriched.enriched_at)}\n`;

      out.write(line);

      if (okCount % 100 === 0) {
        console.log(`Processor: ${okCount} registos enriquecidos...`);
      }
    } catch (err) {
      failCount++;
      logError(errorsRunFile, target, requestId, rawCountry, err?.message || "Erro inesperado");
    }
  }

  await new Promise((resolve) => out.end(resolve));

  console.log(`Processor: enrichment concluido (${okCount} ok, ${failCount} falhas).`);
  console.log(`Processor: CSV enriched guardado em: ${outPath}`);

  // ---- Uploads para o bucket (pastas virtuais)
  const processedBucketPath = `processed/${outName}`;
  await uploadFileToBucket(outPath, processedBucketPath, "text/csv");
  console.log("Processor: enriched enviado para o bucket:", processedBucketPath);

  const errorsBucketPath = `errors/processing_errors_${requestId}.csv`;
  await uploadFileToBucket(errorsRunFile, errorsBucketPath, "text/csv");
  console.log("Processor: erros enviados para o bucket:", errorsBucketPath);

  // 1) enviar para XML Service
  const webhookUrl = `${WEBHOOK_PUBLIC_URL}/webhook/xml-service`;

  console.log("Processor: a enviar para XML Service...");
  const sendRes = await sendToXmlService({
    requestId,
    mapperVersion: MAPPER_VERSION,
    webhookUrl,
    csvPath: outPath,
  });

  if (sendRes?.skipped) {
    console.log("Processor: envio para XML Service ignorado (sem XMLRPC_URL).");

    console.log("Processor: nao vou apagar o CSV do bucket (para nao perder dados).");
    return true;
  }

  console.log("Processor: enviado. A aguardar confirmacao no webhook...");

  // 2) esperar callback do XML Service
  const ack = await waitForWebhookAck(requestId, WEBHOOK_WAIT_SECONDS);

  if (!ack) {
    console.log(
      `Processor: timeout a esperar webhook (${WEBHOOK_WAIT_SECONDS}s). Nao vou apagar o CSV do bucket.`
    );
    return true;
  }

  if (!ack.ok) {
    console.log("Processor: webhook recebido mas com ok=false. Nao vou apagar CSV do bucket.");
    return true;
  }

  console.log("Processor: webhook ok. A apagar CSVs do bucket (original + enriched)...");


    // 3) apagar CSV original (incoming) do bucket quando OK
  await deleteCsvFromBucket(target);
  console.log("Processor: CSV original apagado do bucket:", target);

  // 4) apagar o enriched do bucket também (processed)
  await deleteCsvFromBucket(processedBucketPath);
  console.log("Processor: CSV enriched apagado do bucket:", processedBucketPath);

  // ---- local (prova): NÃO apagar
  console.log("Processor: a manter ficheiros locais para prova:", outPath, errorsRunFile);

  return true;

}

// ------------------------------------------

// ---------------- MAIN LOOP ----------------
async function mainLoop() {
  console.log("Processor a correr...");

  while (true) {
    try {
      let didWork = false;

      while (true) {
        const worked = await runOnce();
        if (!worked) break; // fila vazia
        didWork = true;
      }

      if (!didWork) {
        console.log(`Processor: a aguardar ${INTERVAL_SECONDS} segundos...\n`);
        await sleep(INTERVAL_SECONDS * 1000);
      }
    } catch (e) {
      console.error("Processor erro:", e?.message || e);
      await sleep(INTERVAL_SECONDS * 1000);
    }
  }
}

// ------------------------------------------

// ---------------- START ----------------
app.listen(WEBHOOK_PORT, "0.0.0.0", () => {
  console.log(`Webhook server ativo em ${WEBHOOK_PUBLIC_URL}/webhook/xml-service`);
});

mainLoop();
