import "dotenv/config";
import express from "express";
import axios from "axios";
import { createClient } from "@supabase/supabase-js";
import { parse } from "csv-parse";
import { Readable } from "stream";
import fs from "fs";
import path from "path";

// ---------------- CONFIG ----------------
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const BUCKET = process.env.SUPABASE_BUCKET || "market-csv";

const INTERVAL_SECONDS = Number(process.env.PROCESSOR_INTERVAL_SECONDS || 60);
const WEBHOOK_PORT = Number(process.env.WEBHOOK_PORT || 7070);
// ---------------------------------------

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("Faltam SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY no .env");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const app = express();
app.use(express.json());

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

const ERRORS_DIR = path.join(PROJECT_ROOT, "processor", "errors");
const OUTPUT_DIR = path.join(PROJECT_ROOT, "processor", "output");

const ERRORS_FILE = path.join(ERRORS_DIR, "processing_errors.csv");

if (!fs.existsSync(ERRORS_DIR)) fs.mkdirSync(ERRORS_DIR, { recursive: true });
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

if (!fs.existsSync(ERRORS_FILE)) {
  fs.writeFileSync(ERRORS_FILE, "timestamp,source_file,country,error\n", "utf8");
}

function csvEscape(value) {
  const s = String(value ?? "");
  return `"${s.replace(/"/g, '""')}"`;
}

function logError(sourceFile, country, errorMsg) {
  const line =
    `${csvEscape(new Date().toISOString())},` +
    `${csvEscape(sourceFile)},` +
    `${csvEscape(country)},` +
    `${csvEscape(errorMsg)}\n`;
  fs.appendFileSync(ERRORS_FILE, line, "utf8");
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

// ---------------- WEBHOOK ----------------
app.post("/webhook/xml-service", (req, res) => {
  console.log("Webhook recebido do XML Service:", req.body);
  res.status(200).json({ ok: true });
});
// ----------------------------------------

// ---------------- HELPERS ----------------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function listCsvFiles() {
  const { data, error } = await supabase.storage.from(BUCKET).list("", {
    limit: 100,
    offset: 0,
    sortBy: { column: "name", order: "asc" },
  });

  if (error) throw error;

  return (data || [])
    .map((f) => f.name)
    .filter(
      (name) => name.endsWith(".csv") && name.startsWith("countries_population_")
    )
    .sort();
}

async function downloadCsvStream(filename) {
  const { data, error } = await supabase.storage.from(BUCKET).download(filename);
  if (error) throw error;

  return Readable.fromWeb(data.stream());
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
        `https://restcountries.com/v3.1/name/${encodeURIComponent(
          countryName
        )}?fullText=true`,
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
    throw new Error(`REST Countries falhou: ${getErrorReason(err)}`);
  }

  if (!countryData) return null;

  const iso2 = countryData.cca2 ?? null;
  const iso3 = countryData.cca3 ?? null;

  const currencyCode = countryData.currencies
    ? Object.keys(countryData.currencies)[0]
    : null;

  // API 2: World Bank (usar ISO3)
  let gdpUsd = null;
  let gdpYear = null;

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
      // não rebenta: só fica null, mas vamos registar erro
      throw new Error(`World Bank falhou: ${getErrorReason(err)}`);
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
    mapper_version: "v1",
    enriched_at: new Date().toISOString(),
  };
}
// ------------------------------------------

// ---------------- PIPELINE ----------------
async function runOnce() {
  console.log("Processor: a listar CSVs...");
  const files = await listCsvFiles();

  if (files.length === 0) {
    console.log("Processor: nenhum CSV encontrado.");
    return;
  }

  const target = files[0];
  console.log("Processor: a processar (FIFO):", target);

  const csvStream = await downloadCsvStream(target);

  const parser = csvStream.pipe(
    parse({
      columns: true,
      skip_empty_lines: true,
    })
  );

  const outName = `enriched_${tsCompact()}_${target}`;
  const outPath = path.join(OUTPUT_DIR, outName);

  const out = fs.createWriteStream(outPath, { encoding: "utf8" });
  out.write(
    "countryName,iso2,iso3,populationTotal,worldPct,currencyCode,gdpUsd,gdpYear,mapper_version,enriched_at\n"
  );

  let okCount = 0;
  let failCount = 0;

  for await (const record of parser) {
    try {
      const enriched = await enrichWithApis(record);

      if (!enriched) {
        failCount++;
        logError(target, record.country, "Falha no enrichment (sem match / country inválido)");
        continue;
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
      logError(target, record.country, err.message || "Erro inesperado");
    }
  }

  out.end();

  console.log(
    `Processor: enrichment concluido (${okCount} ok, ${failCount} falhas).`
  );
  console.log(`Processor: CSV enriched guardado em: ${outPath}`);

  // Aqui depois entra o XML Service:
  // - enviar outPath (ou stream) para XML Service
  // - esperar callback no webhook
  // - apagar CSVs
}
// ------------------------------------------

// ---------------- MAIN LOOP ----------------
async function mainLoop() {
  console.log("Processor a correr...");
  while (true) {
    try {
      await runOnce();
    } catch (e) {
      console.error("Processor erro:", e?.message || e);
    }

    console.log(`Processor: a aguardar ${INTERVAL_SECONDS} segundos...\n`);
    await sleep(INTERVAL_SECONDS * 1000);
  }
}
// ------------------------------------------

// ---------------- START ----------------
app.listen(WEBHOOK_PORT, () => {
  console.log(
    `Webhook server ativo em http://localhost:${WEBHOOK_PORT}/webhook/xml-service`
  );
});

mainLoop();
