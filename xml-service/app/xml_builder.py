from __future__ import annotations
import csv
import io
from datetime import datetime, timezone
from lxml import etree


def _first(row: dict, keys: list[str]) -> str:
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return ""


def _safe_int(s: str) -> int | None:
    try:
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _safe_decimal_str(s: str) -> str | None:
    try:
        if not s:
            return None
        return str(float(s))
    except Exception:
        return None


def build_population_report_xml(csv_bytes: bytes, mapper_version: str) -> bytes:
    """
    CSV enriched -> XML (com 1 nível hierárquico).
    Retorna bytes UTF-8 com declaration.
    """
    text = csv_bytes.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    created_at = datetime.now(timezone.utc).isoformat()
    root = etree.Element(
        "PopulationReport",
        mapperVersion=mapper_version,
        createdAt=created_at,
    )

    for row in reader:
        # tenta vários nomes possíveis (flexível)
        name = _first(row, ["countryName", "name", "country", "country_name", "pais", "nome"])

        if not name:
            continue

        internal_id = _first(row, ["internal_id", "id", "csv_id"])

        iso2 = _first(row, ["iso2", "cca2"])
        iso3 = _first(row, ["iso3", "cca3"])

        population_raw = _first(row, ["populationTotal", "population", "population_total", "pop_total"])
        world_pct_raw = _first(row, ["worldPct", "world_percentage", "world_pct", "pct_world"])

        currency = _first(row, ["currencyCode", "currency", "currency_code"])
        gdp_val_raw = _first(row, ["gdpUsd", "gdp_value", "gdp", "gdp_current"])
        gdp_year_raw = _first(row, ["gdpYear", "gdp_year", "year"])


        country_el = etree.SubElement(root, "Country")
        if internal_id:
            country_el.set("internalId", internal_id)

        etree.SubElement(country_el, "Name").text = name

        if iso2:
            etree.SubElement(country_el, "ISO2").text = iso2
        if iso3:
            etree.SubElement(country_el, "ISO3").text = iso3

        pop = _safe_int(population_raw)
        if pop is not None:
            etree.SubElement(country_el, "PopulationTotal").text = str(pop)

        world_pct = _safe_decimal_str(world_pct_raw)
        if world_pct is not None:
            etree.SubElement(country_el, "WorldPct").text = world_pct

        gdp_val = _safe_decimal_str(gdp_val_raw)
        if currency or gdp_val:
            econ_el = etree.SubElement(country_el, "Economy")
            if currency:
                etree.SubElement(econ_el, "Currency").text = currency
            if gdp_val:
                gdp_el = etree.SubElement(econ_el, "GDP")
                if gdp_year_raw.isdigit():
                    gdp_el.set("year", gdp_year_raw)
                gdp_el.text = gdp_val

    return etree.tostring(
        root,
        pretty_print=True,
        xml_declaration=True,
        encoding="UTF-8"
    )


def validate_xml_with_xsd(xml_bytes: bytes, xsd_path: str) -> None:
    """
    Lança ValueError se não validar.
    """
    xml_doc = etree.fromstring(xml_bytes)

    with open(xsd_path, "rb") as f:
        xsd_doc = etree.parse(f)

    schema = etree.XMLSchema(xsd_doc)
    if not schema.validate(xml_doc):
        errors = [str(e) for e in schema.error_log]
        raise ValueError("XSD validation failed: " + " | ".join(errors))
