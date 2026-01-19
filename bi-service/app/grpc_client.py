import os
import grpc
from app import xml_service_pb2, xml_service_pb2_grpc

GRPC_TARGET = os.getenv("XML_GRPC_TARGET", "localhost:50051")

_channel = None
_stub = None

def get_stub():
    global _channel, _stub
    if _stub is None:
        _channel = grpc.insecure_channel(GRPC_TARGET)
        _stub = xml_service_pb2_grpc.XmlQueryServiceStub(_channel)
    return _stub
