import os
import asyncio
import base64
import threading

import psycopg2
import httpx
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

from app.xml_builder import build_population_report_xml, validate_xml_with_xsd

app = FastAPI(title="TP3 XML Service", version="1.0")

# ---------- ENV ----------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

DEFAULT_MAPPER_VERSION = os.getenv("XML_MAPPER_VERSION_DEFAULT", "v1")
XSD_PATH = os.path.join(os.path.dirname(__file__), "xsd", "report_v1.xsd")

# XML-RPC (Processor -> XML Service)
XMLRPC_HOST = os.getenv("XMLRPC_HOST", "0.0.0.0")
XMLRPC_PORT = int(os.getenv("XMLRPC_PORT", "9090"))
# -------------------------


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
    )


async def send_webhook(webhook_url: str, payload: dict):
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(webhook_url, json=payload)
    except Exception:
        pass


async def process_request(request_id: str, webhook_url: str, mapper_version: str, csv_bytes: bytes):
    xml_bytes = build_population_report_xml(csv_bytes, mapper_version)

    try:
        validate_xml_with_xsd(xml_bytes, XSD_PATH)
    except Exception as e:
        payload = {
            "requestId": request_id,
            "ok": False,
            "status": "ERRO_VALIDACAO",
            "documentId": None,
            "mapperVersion": mapper_version,
            "error": str(e),
        }
        await send_webhook(webhook_url, payload)
        return payload, 400

    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO xml_documents (xml_documento, mapper_version)
            VALUES (%s, %s)
            RETURNING id;
            """,
            (xml_bytes.decode("utf-8"), mapper_version),
        )
        doc_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        payload = {
            "requestId": request_id,
            "ok": False,
            "status": "ERRO_PERSISTENCIA",
            "documentId": None,
            "mapperVersion": mapper_version,
            "error": str(e),
        }
        await send_webhook(webhook_url, payload)
        return payload, 500

    payload = {
        "requestId": request_id,
        "ok": True,
        "status": "OK",
        "documentId": doc_id,
        "mapperVersion": mapper_version,
    }
    await send_webhook(webhook_url, payload)
    return payload, 200


@app.get("/health")
def health():
    return {"ok": True}


# REST opcional (debug)
@app.post("/xml")
async def ingest_xml(
    requestId: str = Form(...),
    webhookUrl: str = Form(...),
    mapper_version: str | None = Form(None),
    file: UploadFile = File(...),
):
    mapper_version = mapper_version or DEFAULT_MAPPER_VERSION
    csv_bytes = await file.read()

    payload, status = await process_request(requestId, webhookUrl, mapper_version, csv_bytes)
    if status != 200:
        return JSONResponse(status_code=status, content=payload)
    return payload


# ---------------- XML-RPC SERVER ----------------

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ("/rpc",)

def start_xmlrpc_server():
    server = SimpleXMLRPCServer(
        (XMLRPC_HOST, XMLRPC_PORT),
        requestHandler=RequestHandler,
        allow_none=True,
        logRequests=True,
    )

    def ingestCsv(payload: dict):
        request_id = payload.get("requestId") or payload.get("request_id")
        webhook_url = payload.get("webhookUrl") or payload.get("webhook_url")
        mapper_version = payload.get("mapper_version") or DEFAULT_MAPPER_VERSION
        csv_b64 = payload.get("csv_base64") or ""

        if not request_id or not webhook_url or not csv_b64:
            return {"ok": False, "error": "Missing requestId/webhookUrl/csv_base64"}

        try:
            csv_bytes = base64.b64decode(csv_b64)
        except Exception:
            return {"ok": False, "error": "Invalid base64"}

        # ✅ corre o processamento num loop próprio (thread-safe)
        def _run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(process_request(request_id, webhook_url, mapper_version, csv_bytes))
            loop.close()

        threading.Thread(target=_run, daemon=True).start()

        return {"ok": True, "received": True, "requestId": request_id}

    server.register_function(ingestCsv, "xml.ingestCsv")

    print(f"[XML-SERVICE] XML-RPC a ouvir em http://{XMLRPC_HOST}:{XMLRPC_PORT}/rpc")
    server.serve_forever()


@app.on_event("startup")
async def on_startup():
    t = threading.Thread(target=start_xmlrpc_server, daemon=True)
    t.start()

