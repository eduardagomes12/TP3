import os
import psycopg2
import httpx
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse
from app.xml_builder import build_population_report_xml, validate_xml_with_xsd

app = FastAPI(title="TP3 XML Service", version="1.0")

# ---------- ENV ----------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")  # Supabase normalmente precisa

DEFAULT_MAPPER_VERSION = os.getenv("XML_MAPPER_VERSION_DEFAULT", "v1")
XSD_PATH = os.path.join(os.path.dirname(__file__), "xsd", "report_v1.xsd")
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
    """
    Tenta enviar callback. Se falhar, não rebenta o serviço (mas pode ser logado).
    """
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(webhook_url, json=payload)
    except Exception:
        # aqui podias logar para ficheiro/console
        pass


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/xml")
async def ingest_xml(
    requestId: str = Form(...),
    webhookUrl: str = Form(...),
    mapper_version: str | None = Form(None),
    file: UploadFile = File(...),
):
    mapper_version = mapper_version or DEFAULT_MAPPER_VERSION

    # 1) ler CSV enriched
    csv_bytes = await file.read()

    # 2) gerar XML
    xml_bytes = build_population_report_xml(csv_bytes, mapper_version)

    # 3) validar XSD
    try:
        validate_xml_with_xsd(xml_bytes, XSD_PATH)
    except Exception as e:
        payload = {
            "requestId": requestId,
            "ok": False,
            "status": "ERRO_VALIDACAO",
            "documentId": None,
            "mapperVersion": mapper_version,
            "error": str(e),
        }
        await send_webhook(webhookUrl, payload)
        return JSONResponse(status_code=400, content=payload)

    # 4) persistir em DB (tabela já existe no Supabase)
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
            "requestId": requestId,
            "ok": False,
            "status": "ERRO_PERSISTENCIA",
            "documentId": None,
            "mapperVersion": mapper_version,
            "error": str(e),
        }
        await send_webhook(webhookUrl, payload)
        return JSONResponse(status_code=500, content=payload)

    # 5) webhook callback
    payload = {
        "requestId": requestId,
        "ok": True,
        "status": "OK",
        "documentId": doc_id,
        "mapperVersion": mapper_version,
    }
    await send_webhook(webhookUrl, payload)

    return payload
