import os
import grpc

from app import xml_service_pb2, xml_service_pb2_grpc

XML_GRPC_HOST = os.getenv("XML_GRPC_HOST", "localhost")
XML_GRPC_PORT = int(os.getenv("XML_GRPC_PORT", "50051"))
GRPC_TIMEOUT_SECONDS = float(os.getenv("GRPC_TIMEOUT_SECONDS", "10"))


def _target() -> str:
    return f"{XML_GRPC_HOST}:{XML_GRPC_PORT}"


def countries_by_currency(document_id: int, limit: int = 10):
    target = _target()
    try:
        with grpc.insecure_channel(target) as channel:
            stub = xml_service_pb2_grpc.XmlQueryServiceStub(channel)
            req = xml_service_pb2.CurrencyStatsRequest(documentId=document_id, limit=limit)
            return stub.CountriesByCurrency(req, timeout=GRPC_TIMEOUT_SECONDS)
    except grpc.RpcError as e:
        raise RuntimeError(f"gRPC CountriesByCurrency falhou: {e.details() or str(e)}")


def top_gdp(document_id: int, year: str = "", limit: int = 10):
    target = _target()
    try:
        with grpc.insecure_channel(target) as channel:
            stub = xml_service_pb2_grpc.XmlQueryServiceStub(channel)
            req = xml_service_pb2.TopGdpRequest(documentId=document_id, year=year, limit=limit)
            return stub.TopGdp(req, timeout=GRPC_TIMEOUT_SECONDS)
    except grpc.RpcError as e:
        raise RuntimeError(f"gRPC TopGdp falhou: {e.details() or str(e)}")


def population_filter(document_id: int, min_pop: int, max_pop: int, limit: int = 20):
    target = _target()
    try:
        with grpc.insecure_channel(target) as channel:
            stub = xml_service_pb2_grpc.XmlQueryServiceStub(channel)
            req = xml_service_pb2.PopulationFilterRequest(
                documentId=document_id,
                minPopulation=min_pop,
                maxPopulation=max_pop,
                limit=limit,
            )
            return stub.PopulationFilter(req, timeout=GRPC_TIMEOUT_SECONDS)
    except grpc.RpcError as e:
        raise RuntimeError(f"gRPC PopulationFilter falhou: {e.details() or str(e)}")
