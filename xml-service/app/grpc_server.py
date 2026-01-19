import os
from concurrent import futures

import grpc
from grpc import StatusCode

from app import xml_service_pb2, xml_service_pb2_grpc
from app.xml_queries import top_gdp, countries_by_currency, population_filter

GRPC_HOST = os.getenv("GRPC_HOST", "0.0.0.0")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))


def _require_doc_id(doc_id: int, context):
    if not doc_id or int(doc_id) <= 0:
        context.abort(StatusCode.INVALID_ARGUMENT, "documentId inválido")


class XmlQueryService(xml_service_pb2_grpc.XmlQueryServiceServicer):
    def TopGdp(self, request, context):
        _require_doc_id(request.documentId, context)

        year = request.year if request.year else ""
        limit = request.limit if request.limit else 10

        try:
            items = top_gdp(request.documentId, year, limit)
            return xml_service_pb2.TopGdpResponse(
                items=[
                    xml_service_pb2.TopGdpItem(
                        name=i["name"],
                        iso2=i["iso2"],
                        iso3=i["iso3"],
                        year=i["year"],
                        gdp=float(i["gdp"]),
                    )
                    for i in items
                ]
            )
        except Exception as e:
            context.abort(StatusCode.INTERNAL, f"Erro TopGdp: {str(e)}")

    def CountriesByCurrency(self, request, context):
        _require_doc_id(request.documentId, context)
        limit = request.limit if request.limit else 10

        try:
            items = countries_by_currency(request.documentId, limit)
            return xml_service_pb2.CurrencyStatsResponse(
                items=[
                    xml_service_pb2.CurrencyStat(currency=i["currency"], count=int(i["count"]))
                    for i in items
                ]
            )
        except Exception as e:
            context.abort(StatusCode.INTERNAL, f"Erro CountriesByCurrency: {str(e)}")

    def PopulationFilter(self, request, context):
        _require_doc_id(request.documentId, context)
        limit = request.limit if request.limit else 20

        try:
            items = population_filter(
                request.documentId,
                request.minPopulation,
                request.maxPopulation,
                limit,
            )
            return xml_service_pb2.PopulationFilterResponse(
                items=[
                    xml_service_pb2.PopulationItem(
                        name=i["name"],
                        population=int(i["population"]),
                        worldPct=float(i["worldPct"]),
                    )
                    for i in items
                ]
            )
        except Exception as e:
            context.abort(StatusCode.INTERNAL, f"Erro PopulationFilter: {str(e)}")


def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    xml_service_pb2_grpc.add_XmlQueryServiceServicer_to_server(XmlQueryService(), server)

    bind = f"{GRPC_HOST}:{GRPC_PORT}"
    server.add_insecure_port(bind)

    print(f"[XML-SERVICE] gRPC a ouvir em {bind}")
    server.start()
    server.wait_for_termination()
