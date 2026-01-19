from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from app.grpc_client import (
    countries_by_currency,
    top_gdp,
    population_filter,
)

load_dotenv()

app = FastAPI(title="TP3 BI Service", version="1.0")

# -------------------- CORS --------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5500",
        "http://127.0.0.1:5500",
        "http://localhost",
        "http://127.0.0.1",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ---------------------------------------------


@app.get("/health")
def health():
    return {"ok": True}


# ---------- BI: Currency Stats ----------
@app.get("/bi/currency-stats")
def bi_currency_stats(
    documentId: int = Query(..., ge=1),
    limit: int = Query(10, ge=1, le=200),
):
    try:
        res = countries_by_currency(documentId, limit)
        return {
            "documentId": documentId,
            "items": [
                {"currency": i.currency, "count": i.count}
                for i in res.items
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- BI: Top GDP ----------
@app.get("/bi/top-gdp")
def bi_top_gdp(
    documentId: int = Query(..., ge=1),
    year: str = Query("", max_length=10),
    limit: int = Query(10, ge=1, le=200),
):
    try:
        res = top_gdp(documentId, year, limit)
        return {
            "documentId": documentId,
            "year": year,
            "items": [
                {
                    "name": i.name,
                    "iso2": i.iso2,
                    "iso3": i.iso3,
                    "year": i.year,
                    "gdp": i.gdp,
                }
                for i in res.items
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- BI: Population Filter ----------
@app.get("/bi/population-filter")
def bi_population_filter(
    documentId: int = Query(..., ge=1),
    minPopulation: int = Query(0, ge=0),
    maxPopulation: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=500),
):
    try:
        res = population_filter(
            documentId,
            minPopulation,
            maxPopulation,
            limit,
        )
        return {
            "documentId": documentId,
            "minPopulation": minPopulation,
            "maxPopulation": maxPopulation,
            "items": [
                {
                    "name": i.name,
                    "population": i.population,
                    "worldPct": i.worldPct,
                }
                for i in res.items
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
