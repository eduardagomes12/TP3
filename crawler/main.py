import os
import time
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from supabase import create_client

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population"

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET = os.getenv("SUPABASE_BUCKET", "market-csv")

# Executar de 30 em 30 minutos
INTERVAL_SECONDS = int(os.getenv("CRAWLER_INTERVAL_SECONDS", "60"))

# Manter apenas os últimos 5 CSVs no bucket
MAX_FILES = int(os.getenv("CRAWLER_MAX_FILES", "5"))


def scrape_population_table() -> pd.DataFrame:
    headers = {"User-Agent": "TP3-IS-Scraper/1.0 (educational)"}
    html = requests.get(WIKI_URL, headers=headers, timeout=30).text

    tables = pd.read_html(html)

    target = None
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if "location" in cols and "population" in cols:
            target = t
            break

    if target is None:
        raise RuntimeError("Tabela principal não encontrada (Location/Population).")

    df = target.copy()

    df = df.rename(columns={
        "Location": "country",
        "Population": "population",
        "% of world": "world_pct",
        "Date": "date"
    })

    df = df[["country", "population", "world_pct", "date"]]

    df["country"] = (
        df["country"]
        .astype(str)
        .str.replace(r"\[.*?\]", "", regex=True)
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.strip()
    )

    df["population"] = (
        df["population"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df["population"] = pd.to_numeric(df["population"], errors="coerce").astype("Int64")

    df["world_pct"] = (
        df["world_pct"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    df["world_pct"] = pd.to_numeric(df["world_pct"], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    df = df.dropna(subset=["country", "population"]).reset_index(drop=True)

    return df


def get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Faltam SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY no .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upload_to_supabase(csv_bytes: bytes, filename: str):
    supabase = get_supabase_client()
    supabase.storage.from_(BUCKET).upload(
        filename,
        csv_bytes,
        {"content-type": "text/csv"}
    )


def cleanup_old_files():
    supabase = get_supabase_client()

    # Lista ficheiros no bucket (raiz)
    files = supabase.storage.from_(BUCKET).list(path="")

    csv_files = [
        f for f in files
        if f.get("name", "").endswith(".csv")
        and f.get("name", "").startswith("countries_population_")
    ]

    # Ordena pelo timestamp no nome (YYYYMMDD_HHMMSS)
    csv_files.sort(key=lambda x: x["name"])

    print("Ficheiros CSV no bucket:", [f["name"] for f in csv_files])

    if len(csv_files) <= MAX_FILES:
        print(f"Nao ha nada para apagar (tem {len(csv_files)} <= {MAX_FILES}).")
        return

    to_delete = csv_files[:-MAX_FILES]
    to_delete_names = [f["name"] for f in to_delete]

    print("Vai apagar (FIFO):", to_delete_names)

    # remove espera uma lista de paths
    res = supabase.storage.from_(BUCKET).remove(to_delete_names)
    print("Resultado remove():", res)

    # Confirmacao (lista outra vez)
    files_after = supabase.storage.from_(BUCKET).list(path="")
    names_after = [f.get("name", "") for f in files_after]
    print("Ficheiros depois:", names_after)



def run_once():
    df = scrape_population_table()

    df = pd.concat([df] * 5, ignore_index=True)

    filename = f"countries_population_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    with open(filename, "wb") as f:
        f.write(csv_bytes)

    upload_to_supabase(csv_bytes, filename)
    cleanup_old_files()

    print("CSV enviado:", filename)
    print("Total de linhas:", len(df))


def main():
    while True:
        try:
            print("A executar crawler...")
            run_once()
        except Exception as e:
            print("Erro no crawler:", str(e))

        print(f"A aguardar {INTERVAL_SECONDS} segundos ({INTERVAL_SECONDS // 60} minutos)...\n")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
