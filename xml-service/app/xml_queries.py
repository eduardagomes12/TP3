import os
import psycopg2


# ---------- DB CONFIG ----------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
    )


def _doc_xml_expr(alias="d"):
    """
    Garante que o documento é tratado como XML,
    mesmo que esteja armazenado como TEXT.
    """
    return f"CAST({alias}.xml_documento AS xml)"


# =========================================================
# TOP GDP (deduplicado por país)
# =========================================================
def top_gdp(document_id: int, year: str | None, limit: int = 10):
    year = (year or "").strip()
    limit = int(limit or 10)
    if limit <= 0:
        limit = 10

    sql = f"""
    SELECT DISTINCT ON (x.name)
      x.name,
      x.iso2,
      x.iso3,
      x.gdp_year,
      x.gdp_value
    FROM xml_documents d,
    XMLTABLE(
      '/PopulationReport/Country'
      PASSING {_doc_xml_expr()}
      COLUMNS
        name       text    PATH 'Name',
        iso2       text    PATH 'ISO2',
        iso3       text    PATH 'ISO3',
        gdp_year   text    PATH 'Economy/GDP/@year',
        gdp_value  numeric PATH 'Economy/GDP'
    ) AS x
    WHERE d.id = %s
      AND (%s = '' OR x.gdp_year = %s)
      AND x.gdp_value IS NOT NULL
    ORDER BY x.name, x.gdp_value DESC
    LIMIT %s;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (document_id, year, year, limit))
            rows = cur.fetchall()

    return [
        {
            "name": r[0] or "",
            "iso2": r[1] or "",
            "iso3": r[2] or "",
            "year": r[3] or "",
            "gdp": float(r[4]) if r[4] is not None else 0.0,
        }
        for r in rows
    ]


# =========================================================
# DISTRIBUIÇÃO POR MOEDA
# =========================================================
def countries_by_currency(document_id: int, limit: int = 10):
    limit = int(limit or 10)
    if limit <= 0:
        limit = 10

    sql = f"""
    SELECT
      x.currency,
      COUNT(*)::int AS countries_count
    FROM xml_documents d,
    XMLTABLE(
      '/PopulationReport/Country'
      PASSING {_doc_xml_expr()}
      COLUMNS
        currency text PATH 'Economy/Currency'
    ) AS x
    WHERE d.id = %s
      AND x.currency IS NOT NULL
      AND x.currency <> ''
    GROUP BY x.currency
    ORDER BY countries_count DESC
    LIMIT %s;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (document_id, limit))
            rows = cur.fetchall()

    return [{"currency": r[0], "count": int(r[1])} for r in rows]


# =========================================================
# FILTRO DE POPULAÇÃO (INTERVALO)
# =========================================================
def population_filter(
    document_id: int,
    min_population: int,
    max_population: int,
    limit: int = 20,
):
    min_population = int(min_population or 0)
    max_population = int(max_population or 0)
    limit = int(limit or 20)
    if limit <= 0:
        limit = 20

    # garante intervalo válido
    if max_population and max_population < min_population:
        min_population, max_population = max_population, min_population

    sql = f"""
    SELECT
      x.name,
      x.population_total,
      x.world_pct
    FROM xml_documents d,
    XMLTABLE(
      '/PopulationReport/Country'
      PASSING {_doc_xml_expr()}
      COLUMNS
        name             text    PATH 'Name',
        population_total bigint  PATH 'PopulationTotal',
        world_pct        numeric PATH 'WorldPct'
    ) AS x
    WHERE d.id = %s
      AND x.population_total IS NOT NULL
      AND (
        %s = 0
        OR x.population_total BETWEEN %s AND %s
      )
    ORDER BY COALESCE(x.world_pct, 0) DESC
    LIMIT %s;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (document_id, max_population, min_population, max_population, limit),
            )
            rows = cur.fetchall()

    return [
        {
            "name": r[0] or "",
            "population": int(r[1]) if r[1] is not None else 0,
            "worldPct": float(r[2]) if r[2] is not None else 0.0,
        }
        for r in rows
    ]
