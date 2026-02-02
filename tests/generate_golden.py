"""Generate the expanded golden set of synthetic PDFs and manifest."""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
import csv
from pathlib import Path
from typing import Any, Dict, List

from reportlab.lib.pagesizes import A4  # type: ignore
from reportlab.pdfgen import canvas  # type: ignore

BASE_DIR = Path(__file__).resolve().parents[1]
GOLDEN_DIR = BASE_DIR / "tests" / "golden"
MANIFEST_PATH = BASE_DIR / "tests" / "golden_manifest.csv"

INVOICE_DEFS: List[Dict[str, Any]] = [
    {
        "filename": "golden_01_suministro_iberdrola.pdf",
        "category": "suministros",
        "supplier": "IBERDROLA SUMINISTRO ESPAÑA S.A.U.",
        "nif": "A12345678",
        "invoice_number": "IB-2025-0210",
        "date": "2025-02-10",
        "due_days": "25",
        "concept": "Suministro eléctrico oficinas febrero",
        "base": "180.50",
        "vat": "37.91",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_02_suministro_endesa.pdf",
        "category": "suministros",
        "supplier": "ENDESA ENERGÍA XXI S.L.U.",
        "nif": "B12345001",
        "invoice_number": "EN-2025-0187",
        "date": "2025-02-05",
        "due_days": "20",
        "concept": "Consumo eléctrico enero",
        "base": "215.30",
        "vat": "45.21",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_03_agua_canal.pdf",
        "category": "suministros",
        "supplier": "CANAL DE ISABEL II S.A.",
        "nif": "A84545123",
        "invoice_number": "AG-2025-0103",
        "date": "2025-01-28",
        "due_days": "15",
        "concept": "Suministro de agua oficinas",
        "base": "95.40",
        "vat": "9.54",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_04_gas_naturgy.pdf",
        "category": "suministros",
        "supplier": "NATURGY IBERIA S.A.",
        "nif": "A13579246",
        "invoice_number": "NG-2025-0042",
        "date": "2025-02-12",
        "due_days": "25",
        "concept": "Facturación gas natural febrero",
        "base": "145.80",
        "vat": "30.62",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_05_alquiler_oficina.pdf",
        "category": "alquiler",
        "supplier": "EDIFICIOS CENTRALES SL",
        "nif": "B22223334",
        "invoice_number": "ALQ-2025-0005",
        "date": "2025-02-01",
        "due_days": "30",
        "concept": "Alquiler oficinas mes febrero",
        "base": "1800.00",
        "vat": "378.00",
        "currency": "EUR",
        "tenant": "cliente_b",
    },
    {
        "filename": "golden_06_alquiler_parking.pdf",
        "category": "alquiler",
        "supplier": "PARK&CO SL",
        "nif": "B66778899",
        "invoice_number": "PK-2025-011",
        "date": "2025-02-01",
        "due_days": "30",
        "concept": "Plazas de parking febrero",
        "base": "320.00",
        "vat": "67.20",
        "currency": "EUR",
        "tenant": "cliente_b",
    },
    {
        "filename": "golden_07_software_m365.pdf",
        "category": "software",
        "supplier": "MICROSOFT IBERIA SRL",
        "nif": "B82387121",
        "invoice_number": "MS-2025-9087",
        "date": "2025-02-15",
        "due_days": "30",
        "concept": "Suscripción Microsoft 365",
        "base": "320.00",
        "vat": "67.20",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_08_software_atlassian.pdf",
        "category": "software",
        "supplier": "ATLASSIAN PTY LTD",
        "nif": "EU998877665",
        "invoice_number": "AT-2025-445",
        "date": "2025-02-11",
        "due_days": "30",
        "concept": "Licencias Jira/Confluence",
        "base": "250.00",
        "vat": "52.50",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_09_servicio_consultoria.pdf",
        "category": "servicios_prof",
        "supplier": "CONSULTORES CLOUD SA",
        "nif": "A55667788",
        "invoice_number": "CC-2025-012",
        "date": "2025-01-29",
        "due_days": "30",
        "concept": "Consultoría tecnológica Enero",
        "base": "1500.00",
        "vat": "315.00",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_10_servicio_asesoria.pdf",
        "category": "servicios_prof",
        "supplier": "ASESORES MADRID SL",
        "nif": "B44556677",
        "invoice_number": "ASE-2025-045",
        "date": "2025-02-05",
        "due_days": "30",
        "concept": "Honorarios asesoría fiscal",
        "base": "420.00",
        "vat": "88.20",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_11_hosteleria_catering.pdf",
        "category": "hosteleria",
        "supplier": "CATERING DELUXE SL",
        "nif": "B10293847",
        "invoice_number": "CAT-2025-19",
        "date": "2025-02-14",
        "due_days": "15",
        "concept": "Catering comité dirección",
        "currency": "EUR",
        "tenant": "demo",
        "lines": [
            {"desc": "Catering menú degustación", "base": "280.00", "vat_rate": 21},
            {"desc": "Bebidas reducidas", "base": "140.00", "vat_rate": 10},
        ],
    },
    {
        "filename": "golden_12_hosteleria_evento.pdf",
        "category": "hosteleria",
        "supplier": "RESTAURANTE TAPAS MADRID SL",
        "nif": "B11223344",
        "invoice_number": "BTM-2025-33",
        "date": "2025-02-07",
        "due_days": "10",
        "concept": "Comida con cliente estratégico",
        "currency": "EUR",
        "tenant": "demo",
        "lines": [
            {"desc": "Degustación gourmet", "base": "120.00", "vat_rate": 21},
            {"desc": "Bebidas especiales", "base": "60.00", "vat_rate": 10},
        ],
    },
    {
        "filename": "golden_13_intracom_hubspot.pdf",
        "category": "intracomunitaria",
        "supplier": "HUBSPOT IRELAND LTD",
        "nif": "EU372000579",
        "invoice_number": "HS-2025-653",
        "date": "2025-02-03",
        "due_days": "30",
        "concept": "Suscripción CRM Hubspot (IVA 0%)",
        "currency": "EUR",
        "tenant": "demo",
        "lines": [
            {"desc": "Plan Enterprise", "base": "540.00", "vat_rate": 0},
        ],
    },
    {
        "filename": "golden_14_intracom_dropbox.pdf",
        "category": "intracomunitaria",
        "supplier": "DROPBOX INTERNATIONAL UNLIMITED COMPANY",
        "nif": "EU372000988",
        "invoice_number": "DB-2025-210",
        "date": "2025-02-09",
        "due_days": "30",
        "concept": "Suscripción Dropbox Advanced",
        "currency": "EUR",
        "tenant": "demo",
        "lines": [
            {"desc": "Dropbox Advanced", "base": "199.00", "vat_rate": 0},
        ],
    },
    {
        "filename": "golden_15_abono_material.pdf",
        "category": "abono",
        "supplier": "PAPELERÍA CENTRAL SL",
        "nif": "B99887766",
        "invoice_number": "ABN-2025-07",
        "date": "2025-02-16",
        "due_days": "0",
        "concept": "Abono devolución material defectuoso",
        "base": "-120.00",
        "vat": "-25.20",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_16_abono_servicios.pdf",
        "category": "abono",
        "supplier": "CONSULTORES CLOUD SA",
        "nif": "A55667788",
        "invoice_number": "ABN-2025-13",
        "date": "2025-02-18",
        "due_days": "0",
        "concept": "Nota de crédito horas no empleadas",
        "base": "-300.00",
        "vat": "-63.00",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_17_marketing_googleads.pdf",
        "category": "marketing",
        "supplier": "GOOGLE IRELAND LTD",
        "nif": "EU372000041",
        "invoice_number": "AD-2025-332",
        "date": "2025-02-11",
        "due_days": "30",
        "concept": "Campaña Google Ads febrero",
        "base": "890.00",
        "vat": "0.00",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_18_telefonia_vodafone.pdf",
        "category": "telefonia",
        "supplier": "VODAFONE ESPAÑA SAU",
        "nif": "A80907397",
        "invoice_number": "VF-2025-229",
        "date": "2025-02-06",
        "due_days": "20",
        "concept": "Telefonía y datos móviles febrero",
        "base": "260.00",
        "vat": "54.60",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_19_seguros_mapfre.pdf",
        "category": "seguros",
        "supplier": "MAPFRE EMPRESAS SA",
        "nif": "A28141935",
        "invoice_number": "SEG-2025-004",
        "date": "2025-01-20",
        "due_days": "30",
        "concept": "Seguro multirriesgo anual (primer pago)",
        "base": "640.00",
        "vat": "134.40",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_20_material_oficina.pdf",
        "category": "material_oficina",
        "supplier": "OFIMARKET SL",
        "nif": "B33445566",
        "invoice_number": "MAT-2025-055",
        "date": "2025-02-08",
        "due_days": "20",
        "concept": "Material de oficina febrero",
        "base": "210.00",
        "vat": "44.10",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_21_viajes_hoteles.pdf",
        "category": "viajes",
        "supplier": "HOTELES IBERIA SL",
        "nif": "B44552211",
        "invoice_number": "HT-2025-021",
        "date": "2025-02-02",
        "due_days": "15",
        "concept": "Alojamiento equipo comercial",
        "base": "560.00",
        "vat": "56.00",
        "currency": "EUR",
        "tenant": "cliente_b",
    },
    {
        "filename": "golden_22_mantenimiento_limpieza.pdf",
        "category": "mantenimiento",
        "supplier": "LIMPIEZAS BRILLAR SL",
        "nif": "B22334455",
        "invoice_number": "LIM-2025-15",
        "date": "2025-02-12",
        "due_days": "15",
        "concept": "Servicio limpieza y mantenimiento oficinas",
        "base": "480.00",
        "vat": "100.80",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_23_formacion_online.pdf",
        "category": "formacion",
        "supplier": "PLATAFORMA FORMACIÓN ONLINE SL",
        "nif": "B55667744",
        "invoice_number": "FOR-2025-08",
        "date": "2025-02-04",
        "due_days": "30",
        "concept": "Formación online equipos soporte",
        "base": "350.00",
        "vat": "73.50",
        "currency": "EUR",
        "tenant": "demo",
    },
    {
        "filename": "golden_24_it_support.pdf",
        "category": "it_support",
        "supplier": "TECH SUPPORT SERVICES SL",
        "nif": "B90909090",
        "invoice_number": "IT-2025-101",
        "date": "2025-02-10",
        "due_days": "30",
        "concept": "Soporte IT y monitorización infra",
        "base": "780.00",
        "vat": "163.80",
        "currency": "EUR",
        "tenant": "demo",
    },
    # --- Ventas (AR) ---
    {
        "filename": "golden_25_ventas_servicios_consultoria.pdf",
        "category": "ventas_servicios",
        "supplier": "CLIENTE INNOVA SL",
        "nif": "B76543210",
        "invoice_number": "VS-2025-050",
        "date": "2025-02-05",
        "due_days": "30",
        "concept": "Servicios de consultoría febrero",
        "base": "2500.00",
        "vat": "525.00",
        "currency": "EUR",
        "tenant": "demo",
        "flow": "AR",
        "notes": "AR: venta servicios",
    },
    {
        "filename": "golden_26_ventas_productos_material.pdf",
        "category": "ventas_productos",
        "supplier": "DISTRIBUCIONES ATLANTICO SA",
        "nif": "A44556677",
        "invoice_number": "VP-2025-014",
        "date": "2025-02-08",
        "due_days": "45",
        "concept": "Venta material promocional",
        "currency": "EUR",
        "tenant": "demo",
        "flow": "AR",
        "lines": [
            {"desc": "Material merchandising", "base": "1400.00", "vat_rate": 21},
            {"desc": "Servicios montaje", "base": "400.00", "vat_rate": 10},
        ],
        "notes": "AR: multi IVA",
    },
    {
        "filename": "golden_27_ventas_intracom_saas.pdf",
        "category": "ventas_intracom",
        "supplier": "EUROPE TECH GMBH",
        "nif": "EU123456789",
        "invoice_number": "VI-2025-007",
        "date": "2025-02-03",
        "due_days": "30",
        "concept": "Licencias SaaS intracomunitarias",
        "base": "1200.00",
        "vat": "0.00",
        "currency": "EUR",
        "tenant": "demo",
        "flow": "AR",
        "notes": "AR: intracom",
    },
    {
        "filename": "golden_28_ventas_abono_servicios.pdf",
        "category": "ventas_abono",
        "supplier": "CLIENTE INNOVA SL",
        "nif": "B76543210",
        "invoice_number": "VA-2025-003",
        "date": "2025-02-20",
        "due_days": "0",
        "concept": "Abono por servicios no prestados",
        "base": "-500.00",
        "vat": "-105.00",
        "currency": "EUR",
        "tenant": "demo",
        "flow": "AR",
        "notes": "AR: nota crédito",
    },
    {
        "filename": "golden_29_ventas_servicios_cliente_b.pdf",
        "category": "ventas_servicios",
        "supplier": "CLIENTE NORTE CONSULTING SL",
        "nif": "B90807060",
        "invoice_number": "VS-2025-081",
        "date": "2025-02-12",
        "due_days": "30",
        "concept": "Servicios soporte continuo",
        "base": "1800.00",
        "vat": "378.00",
        "currency": "EUR",
        "tenant": "cliente_b",
        "flow": "AR",
    },
    {
        "filename": "golden_30_ventas_producto_export.pdf",
        "category": "ventas_intracom",
        "supplier": "ALPINE RETAIL GMBH",
        "nif": "EU998822110",
        "invoice_number": "VI-2025-011",
        "date": "2025-02-15",
        "due_days": "45",
        "concept": "Suministro hardware intracomunitario",
        "currency": "EUR",
        "tenant": "demo",
        "flow": "AR",
        "lines": [
            {"desc": "Lote hardware", "base": "2100.00", "vat_rate": 0},
        ],
        "notes": "AR: export",
    },
    {
        "filename": "golden_31_ventas_ticket_evento.pdf",
        "category": "ventas_ticket",
        "supplier": "CLIENTE EVENTOS EXPRESS",
        "nif": "B60708090",
        "invoice_number": "VT-2025-030",
        "date": "2025-02-06",
        "due_days": "15",
        "concept": "Venta entradas evento interno",
        "base": "420.00",
        "vat": "88.20",
        "currency": "EUR",
        "tenant": "demo",
        "flow": "AR",
    },
    {
        "filename": "golden_32_ventas_productos_minorista.pdf",
        "category": "ventas_productos",
        "supplier": "CLIENTE RETAIL BOUTIQUE",
        "nif": "B12003450",
        "invoice_number": "VP-2025-020",
        "date": "2025-02-18",
        "due_days": "20",
        "concept": "Venta productos línea premium",
        "base": "950.00",
        "vat": "199.50",
        "currency": "EUR",
        "tenant": "demo",
        "flow": "AR",
    },
    {
        "filename": "golden_33_ventas_servicios_multiiva.pdf",
        "category": "ventas_servicios",
        "supplier": "CONSORCIO EUROPEO",
        "nif": "EU445566778",
        "invoice_number": "VS-2025-095",
        "date": "2025-02-10",
        "due_days": "30",
        "concept": "Servicios híbridos",
        "currency": "EUR",
        "tenant": "demo",
        "flow": "AR",
        "lines": [
            {"desc": "Consultoría", "base": "600.00", "vat_rate": 21},
            {"desc": "Formación", "base": "300.00", "vat_rate": 10},
        ],
    },
    {
        "filename": "golden_34_ventas_abono_producto.pdf",
        "category": "ventas_abono",
        "supplier": "DISTRIBUCIONES ATLANTICO SA",
        "nif": "A44556677",
        "invoice_number": "VA-2025-006",
        "date": "2025-02-22",
        "due_days": "0",
        "concept": "Abono devolución producto",
        "base": "-300.00",
        "vat": "-63.00",
        "currency": "EUR",
        "tenant": "demo",
        "flow": "AR",
    },
]


def _ensure_dirs() -> None:
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    for pdf in GOLDEN_DIR.glob("*.pdf"):
        pdf.unlink()


def _draw_invoice(pdf_path: Path, info: Dict[str, Any]) -> None:
    c = canvas.Canvas(str(pdf_path), pagesize=A4)
    y = 800
    lines = [
        "Factura CERTIVA Golden",
        f"Categoría: {info['category']}",
        f"Flujo: {info.get('flow', 'AP')}",
        f"Proveedor: {info['supplier']}",
        f"NIF/VAT: {info['nif']}",
        f"Factura: {info['invoice_number']}",
        f"Fecha: {info['date']}  Vencimiento: {info['due_date']}",
        f"Concepto: {info['concept']}",
        f"Base imponible: {info['base']} {info['currency']}",
        f"IVA: {info['vat']} {info['currency']}",
        f"Total: {info['gross']} {info['currency']}",
        f"Tenant: {info['tenant']}",
    ]
    if info.get("lines"):
        lines.append("----")
        lines.append("Desglose líneas:")
        for line in info["lines"]:
            lines.append(
                f"  - {line['desc']} | Base {line['base']} | IVA {line.get('vat_rate', 21)}%"
            )
    for line in lines:
        c.drawString(40, y, line)
        y -= 20
    c.showPage()
    c.save()


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    base = Decimal(row["base"])
    vat = Decimal(row["vat"])
    gross = Decimal(row.get("gross") or (base + vat))
    row["gross"] = f"{gross:.2f}"
    row.setdefault("vat_breakdown", row.get("vat_breakdown", ""))
    return row


def generate() -> None:
    _ensure_dirs()
    manifest_rows: List[Dict[str, Any]] = []
    for data in INVOICE_DEFS:
        base_date = datetime.fromisoformat(data["date"])
        due = base_date + timedelta(days=int(data["due_days"]))
        record = {
            "filename": data["filename"],
            "category": data["category"],
            "supplier": data["supplier"],
            "nif": data["nif"],
            "invoice_number": data["invoice_number"],
            "date": data["date"],
            "due_date": due.date().isoformat(),
            "concept": data["concept"],
            "currency": data.get("currency", "EUR"),
            "tenant": data.get("tenant", "demo"),
            "notes": data.get("notes", ""),
            "lines": data.get("lines", []),
            "flow": data.get("flow", "AP"),
        }
        if data.get("lines"):
            base_total = Decimal("0")
            vat_total = Decimal("0")
            breakdown_parts = []
            for line in data["lines"]:
                base_val = Decimal(str(line["base"]))
                rate = Decimal(str(line.get("vat_rate", 21)))
                vat_val = (rate / Decimal("100")) * base_val
                base_total += base_val
                vat_total += vat_val
                breakdown_parts.append(f"{rate}:{base_val}")
            record["base"] = f"{base_total:.2f}"
            record["vat"] = f"{vat_total:.2f}"
            record["gross"] = f"{(base_total + vat_total):.2f}"
            record["vat_breakdown"] = "|".join(breakdown_parts)
        else:
            gross = Decimal(data["base"]) + Decimal(data["vat"])
            if "gross" in data:
                gross = Decimal(data["gross"])
            record["base"] = data["base"]
            record["vat"] = data["vat"]
            record["gross"] = f"{gross:.2f}"
            record["vat_breakdown"] = ""
        pdf_path = GOLDEN_DIR / data["filename"]
        _draw_invoice(pdf_path, record)
        manifest_rows.append(record)

    with MANIFEST_PATH.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "filename",
                "category",
                "tenant",
                "supplier",
                "nif",
                "invoice_number",
                "date",
                "due_date",
                "concept",
                "base",
                "vat",
                "gross",
                "currency",
                "vat_breakdown",
                "flow",
                "notes",
            ],
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in manifest_rows:
            writer.writerow(_normalize_row(row))
    print(f"Generadas {len(manifest_rows)} facturas en {GOLDEN_DIR}")


if __name__ == "__main__":
    generate()
