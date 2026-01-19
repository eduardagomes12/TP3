import os
from fastapi import FastAPI, HTTPException, Query
from app.grpc_client import get_stub
from app import xml_service_pb2
import grpc

app = FastAPI(title="TP3 BI Service", version="1.0")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/bi/{document_id}/currency-stats")
def currency_stats(document_id: int, limit: int = Query(10, ge=1, le=200)):
    try:
        stub = get_stub()
        req = xml_service_pb2.CurrencyStatsRequest(documentId=document_id, limit=limit)
        res = stub.CountriesByCurrency(req)

        return {
            "documentId": document_id,
            "items": [{"currency": i.currency, "count": i.count} for i in res.items],
        }
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=e.details() or "Erro gRPC")

@app.get("/bi/{document_id}/top-gdp")
def top_gdp(
    document_id: int,
    year: str = Query("", max_length=10),
    limit: int = Query(10, ge=1, le=200),
):
    try:
        stub = get_stub()
        req = xml_service_pb2.TopGdpRequest(documentId=document_id, year=year, limit=limit)
        res = stub.TopGdp(req)

        return {
            "documentId": document_id,
            "year": year,
            "items": [
                {"name": i.name, "iso2": i.iso2, "iso3": i.iso3, "year": i.year, "gdp": i.gdp}
                for i in res.items
            ],
        }
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=e.details() or "Erro gRPC")

@app.get("/bi/{document_id}/population-filter")
def population_filter(
    document_id: int,
    min_population: int = Query(0, ge=0),
    max_population: int = Query(2_000_000_000, ge=0),
    limit: int = Query(20, ge=1, le=200),
):
    try:
        stub = get_stub()
        req = xml_service_pb2.PopulationFilterRequest(
            documentId=document_id,
            minPopulation=min_population,
            maxPopulation=max_population,
            limit=limit,
        )
        res = stub.PopulationFilter(req)

        return {
            "documentId": document_id,
            "items": [
                {"name": i.name, "population": i.population, "worldPct": i.worldPct}
                for i in res.items
            ],
        }
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=e.details() or "Erro gRPC")
