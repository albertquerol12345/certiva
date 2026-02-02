"""Offline generator for Facturae XML files."""
from __future__ import annotations

from decimal import Decimal
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Optional

from lxml import etree

from . import utils
from .config import settings

FACTURAE_DIR = utils.BASE_DIR / "OUT" / "facturae"
FACTURAE_DIR.mkdir(parents=True, exist_ok=True)
FACTURAE_XSD_PATH = utils.BASE_DIR / "data" / "xsd" / "facturae_3_2_2.xsd"
_FACTURAE_SCHEMA: Optional[etree.XMLSchema] = None


def _load_doc(doc_id: str) -> Dict:
    doc = utils.get_doc(doc_id)
    if not doc:
        raise ValueError(f"No existe el documento {doc_id}")
    normalized = utils.read_json(utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json")
    entry_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.entry.json"
    entry = utils.read_json(entry_path) if entry_path.exists() else {}
    metadata = normalized.get("metadata") or {}
    flow = (metadata.get("flow") or ("AR" if (doc["doc_type"] or "").startswith("sales") else "AP")).upper()
    if flow != "AR":
        raise ValueError("Facturae solo se genera para facturas de venta (flow=AR)")
    return {"doc": doc, "normalized": normalized, "entry": entry}


def _validate_totals(totals: Dict[str, float]) -> None:
    if not totals:
        raise ValueError("Totales de la factura no encontrados.")
    base = Decimal(str(totals.get("base", 0)))
    vat = Decimal(str(totals.get("vat", 0)))
    gross = Decimal(str(totals.get("gross", 0)))
    if abs((base + vat) - gross) > Decimal("0.01"):
        raise ValueError("Totales incoherentes: base + IVA debe ser igual al total.")


def _get_facturae_schema() -> etree.XMLSchema:
    global _FACTURAE_SCHEMA
    if _FACTURAE_SCHEMA is None:
        if not FACTURAE_XSD_PATH.exists():
            raise ValueError(f"No se encontró el XSD Facturae en {FACTURAE_XSD_PATH}")
        schema_doc = etree.parse(str(FACTURAE_XSD_PATH))
        _FACTURAE_SCHEMA = etree.XMLSchema(schema_doc)
    return _FACTURAE_SCHEMA


def validate_facturae_xml(xml_str: str) -> None:
    schema = _get_facturae_schema()
    try:
        document = etree.fromstring(xml_str.encode("utf-8"))
    except etree.XMLSyntaxError as exc:
        raise ValueError(f"XML Facturae mal formado: {exc}") from exc
    if not schema.validate(document):
        errors = "; ".join(err.message for err in schema.error_log[:3])
        raise ValueError(f"Facturae XML inválido: {errors or 'revisa la estructura generada'}")


def build_facturae_xml(doc_id: str) -> str:
    bundle = _load_doc(doc_id)
    normalized = bundle["normalized"]
    entry = bundle["entry"]
    invoice = normalized.get("invoice") or {}
    totals = normalized.get("totals") or {}
    metadata = normalized.get("metadata") or {}
    customer = normalized.get("customer") or normalized.get("supplier") or {}
    lines = normalized.get("lines") or []
    if not lines:
        lines = [
            {
                "desc": metadata.get("category") or "Concepto",
                "qty": 1,
                "amount": totals.get("base", 0.0),
                "vat_rate": metadata.get("vat_rate") or totals.get("vat_rate") or 21.0,
            }
        ]
    if not invoice.get("date"):
        raise ValueError("La factura debe tener fecha de emisión para generar Facturae.")
    _validate_totals(totals)

    root = ET.Element("Facturae", attrib={"Version": "3.2.2"})
    parties = ET.SubElement(root, "Parties")
    seller = ET.SubElement(parties, "SellerParty")
    ET.SubElement(seller, "TaxIdentificationNumber").text = settings.facturae_tax_id
    ET.SubElement(seller, "CorporateName").text = settings.facturae_name
    address = ET.SubElement(seller, "AddressInSpain")
    ET.SubElement(address, "Address").text = settings.facturae_address
    ET.SubElement(address, "PostCode").text = settings.facturae_postal_code
    ET.SubElement(address, "CountryCode").text = settings.facturae_country_code

    buyer = ET.SubElement(parties, "BuyerParty")
    ET.SubElement(buyer, "TaxIdentificationNumber").text = customer.get("nif") or "ES00000000B"
    ET.SubElement(buyer, "CorporateName").text = customer.get("name") or "Cliente Demo"

    invoices = ET.SubElement(root, "Invoices")
    invoice_node = ET.SubElement(invoices, "Invoice")
    ET.SubElement(invoice_node, "InvoiceNumber").text = invoice.get("number") or doc_id
    ET.SubElement(invoice_node, "InvoiceSeriesCode").text = invoice.get("series") or "A"
    ET.SubElement(invoice_node, "InvoiceIssueDate").text = invoice.get("date")

    items = ET.SubElement(invoice_node, "Items")
    for line in lines:
        it = ET.SubElement(items, "Item")
        ET.SubElement(it, "Description").text = line.get("desc") or "Concepto"
        ET.SubElement(it, "Quantity").text = str(line.get("qty") or 1)
        ET.SubElement(it, "UnitPriceWithoutTax").text = f"{float(line.get('amount', 0.0)):.2f}"
        ET.SubElement(it, "TaxRate").text = f"{float(line.get('vat_rate', 21.0)):.2f}"

    taxes = ET.SubElement(invoice_node, "TaxesOutputs")
    tax = ET.SubElement(taxes, "Tax")
    ET.SubElement(tax, "TaxRate").text = f"{float(totals.get('vat_rate', 21.0)):.2f}"
    ET.SubElement(tax, "TaxableBase").text = f"{float(totals.get('base', 0.0)):.2f}"
    ET.SubElement(tax, "TaxAmount").text = f"{float(totals.get('vat', 0.0)):.2f}"

    totals_node = ET.SubElement(invoice_node, "InvoiceTotals")
    ET.SubElement(totals_node, "TotalGrossAmount").text = f"{float(totals.get('base', 0.0)):.2f}"
    ET.SubElement(totals_node, "TotalTaxOutputs").text = f"{float(totals.get('vat', 0.0)):.2f}"
    ET.SubElement(totals_node, "TotalInvoiceAmount").text = f"{float(totals.get('gross', 0.0)):.2f}"

    payments = ET.SubElement(invoice_node, "PaymentDetails")
    payment = ET.SubElement(payments, "PaymentDetail")
    ET.SubElement(payment, "PaymentMeans").text = "31"  # transferencia
    ET.SubElement(payment, "PaymentAmount").text = f"{float(totals.get('gross', 0.0)):.2f}"
    ET.SubElement(payment, "PaymentDueDate").text = invoice.get("due") or invoice.get("date")

    # Serialize pretty
    xml_bytes = ET.tostring(root, encoding="utf-8")
    return xml_bytes.decode("utf-8")


def write_facturae_file(doc_id: str, path: Optional[Path] = None) -> Path:
    xml_content = build_facturae_xml(doc_id)
    validate_facturae_xml(xml_content)
    target = path or (FACTURAE_DIR / f"{doc_id}.xml")
    target.write_text(xml_content, encoding="utf-8")
    return target
