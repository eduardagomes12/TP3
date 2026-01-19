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

# Executar periodicamente (1800s)
INTERVAL_SECONDS = int(os.getenv("CRAWLER_INTERVAL_SECONDS", "60"))



# Pasta local para guardar CSVs gerados
EXPORT_DIR = os.path.join(os.path.dirname(__file__), "exports", "incoming")



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

    # Renomear colunas
    df = df.rename(columns={
        "Location": "country",
        "Population": "population",
        "% of world": "world_pct",
        "Date": "date"  # pode existir na tabela, mas vamos remover depois
    })

    # Queremos só estas 3 colunas (SEM date)
    df = df[["country", "population", "world_pct"]]

    # Limpar country
    df["country"] = (
        df["country"]
        .astype(str)
        .str.replace(r"\[.*?\]", "", regex=True)
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.strip()
    )

    # Limpar population
    df["population"] = (
        df["population"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df["population"] = pd.to_numeric(df["population"], errors="coerce").astype("Int64")

    # Limpar world_pct
    df["world_pct"] = (
        df["world_pct"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    df["world_pct"] = pd.to_numeric(df["world_pct"], errors="coerce")

    # Remover linhas inválidas
    df = df.dropna(subset=["country", "population"]).reset_index(drop=True)

    return df


def get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Faltam SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY no .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upload_to_supabase(csv_bytes: bytes, filename: str):
    supabase = get_supabase_client()
    supabase.storage.from_(BUCKET).upload(
        path=filename,
        file=csv_bytes,
        file_options={
            "content-type": "text/csv",
            "upsert": "true"   
        }
    )







def run_once():
    df = scrape_population_table()

    # Multiplicar por 5 (mais volume)
    df = pd.concat([df] * 5, ignore_index=True)

    filename = f"countries_population_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    # Garantir pasta exports existe
    os.makedirs(EXPORT_DIR, exist_ok=True)

    # Guardar localmente dentro de crawler/exports/
    local_path = os.path.join(EXPORT_DIR, filename)
    with open(local_path, "wb") as f:
        f.write(csv_bytes)

    # Upload para Supabase
    bucket_path = f"incoming/{filename}"
    upload_to_supabase(csv_bytes, bucket_path)

  

    print("CSV enviado:", bucket_path)

    print("Total de linhas:", len(df))
    print("Guardado localmente em:", local_path)


def main():
    while True:
        try:
            print("A executar crawler...")
            run_once()
        except Exception as e:
            print("Erro no crawler:", str(e))

        mins = INTERVAL_SECONDS // 60
        print(f"A aguardar {INTERVAL_SECONDS} segundos ({mins} min)...\n")

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
