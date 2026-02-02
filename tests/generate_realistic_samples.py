from __future__ import annotations

import argparse
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from reportlab.lib import colors  # type: ignore
from reportlab.lib.pagesizes import A4  # type: ignore
from reportlab.lib.units import mm  # type: ignore
from reportlab.pdfgen import canvas  # type: ignore

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_TESTS = BASE_DIR / "tests" / "realistic_big"
DEFAULT_IN = BASE_DIR / "IN" / "lote_sintetico_grande"

CUSTOMER = {
    "name": "CERTIVA DEMO SL",
    "nif": "B00000000",
    "address": "C/ Alcalá 123, 28009 Madrid",
    "iban": "ES12 3456 7890 1234 5678 9012",
}

MONTHS = [
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre",
]


SUPPLIERS: List[Dict] = [
    {
        "name": "IBERDROLA COMERCIALIZACIÓN ELÉCTRICA SAU",
        "nif": "A95758389",
        "category": "suministros_luz",
        "base_range": (80, 450),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 3,
        "concepts": [
            "Consumo eléctrico {month}",
            "Potencia contratada",
            "Alquiler contador inteligente",
        ],
    },
    {
        "name": "ENDESA ENERGIA XXI SL",
        "nif": "B82846825",
        "category": "suministros_luz",
        "base_range": (90, 500),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 3,
        "concepts": [
            "Consumo mensual {month}",
            "Servicios auxiliares",
        ],
    },
    {
        "name": "CANAL DE ISABEL II SA",
        "nif": "A28012685",
        "category": "agua",
        "base_range": (40, 200),
        "vat_rates": [10],
        "min_lines": 1,
        "max_lines": 2,
        "concepts": [
            "Consumo de agua {month}",
            "Canon de saneamiento",
        ],
    },
    {
        "name": "VODAFONE ESPAÑA SAU",
        "nif": "A80907397",
        "category": "telefonia",
        "base_range": (35, 220),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 4,
        "concepts": [
            "Línea móvil corporativa",
            "Tarifa de datos ilimitados",
            "Servicios adicionales roaming",
        ],
    },
    {
        "name": "MOVISTAR ESPAÑA SL",
        "nif": "B84448048",
        "category": "telefonia",
        "base_range": (30, 200),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 3,
        "concepts": [
            "Fibra + Móvil Pro",
            "Servicios avanzados centralita virtual",
        ],
    },
    {
        "name": "MICROSOFT IBERIA SRL",
        "nif": "B82387121",
        "category": "software",
        "base_range": (120, 1500),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 4,
        "concepts": [
            "Suscripción Microsoft 365",
            "Licencias Azure servicios PaaS",
            "Visual Studio Enterprise",
        ],
    },
    {
        "name": "HUBSPOT IRELAND LTD",
        "nif": "EU372000579",
        "category": "software_intracom",
        "base_range": (200, 2000),
        "vat_rates": [0],
        "min_lines": 1,
        "max_lines": 3,
        "concepts": [
            "Suscripción HubSpot Pro",
            "Onboarding servicios SaaS",
        ],
    },
    {
        "name": "CATERING GOURMET SL",
        "nif": "B66554433",
        "category": "hosteleria",
        "base_range": (50, 600),
        "vat_rates": [10, 21],
        "min_lines": 2,
        "max_lines": 5,
        "concepts": [
            "Servicio de catering evento",
            "Alquiler de menaje",
            "Bebidas premium",
        ],
    },
    {
        "name": "ALQUILERES CENTRO SL",
        "nif": "B11224455",
        "category": "alquiler",
        "base_range": (400, 2500),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 1,
        "concepts": ["Renta mensual oficina"],
    },
    {
        "name": "GAS NATURAL SDG SA",
        "nif": "A13579246",
        "category": "suministros_gas",
        "base_range": (60, 320),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 3,
        "concepts": ["Consumo gas natural {month}", "Alquiler contador"],
    },
    {
        "name": "REST. EL PUERTO",
        "nif": "E1234567A",
        "category": "hosteleria_tickets",
        "base_range": (15, 90),
        "vat_rates": [10],
        "min_lines": 1,
        "max_lines": 3,
        "concepts": ["Servicio restaurante {month}", "Menú ejecutivo", "Café / sobremesa"],
    },
    {
        "name": "PUBLICIDAD CREATIVA SL",
        "nif": "B99887766",
        "category": "abono_marketing",
        "base_range": (100, 900),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 2,
        "concepts": ["Abono campaña redes", "Regularización servicios marketing"],
        "is_credit": True,
    },
    {
        "name": "ESTACION SERVICIO CARRETERA SA",
        "nif": "B66778899",
        "category": "gasolina",
        "base_range": (40, 180),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 3,
        "concepts": ["Gasolina 95", "Lavado premium", "AdBlue vehículos"],
    },
    {
        "name": "SIA CLOUD SECURITY SL",
        "nif": "B44556677",
        "category": "consultoria",
        "base_range": (500, 3500),
        "vat_rates": [21],
        "min_lines": 1,
        "max_lines": 4,
        "concepts": [
            "Auditoría ciberseguridad",
            "Servicios consultoría {month}",
            "Monitorización SOC",
        ],
    },
]


def _split_amount(total: float, parts: int, rng: random.Random) -> List[float]:
    weights = [rng.random() for _ in range(parts)]
    weight_sum = sum(weights) or 1.0
    allocated = []
    for i in range(parts):
        portion = total * (weights[i] / weight_sum)
        allocated.append(portion)
    # ajustar rounding para que sumen total
    rounded = [round(x, 2) for x in allocated]
    diff = round(total - sum(rounded), 2)
    if rounded:
        rounded[-1] = round(rounded[-1] + diff, 2)
    return rounded


def _build_invoice(idx: int, rng: random.Random) -> Dict:
    supplier = rng.choice(SUPPLIERS)
    invoice_date = date(2025, 1, 1) + timedelta(days=rng.randint(0, 90))
    due_date = invoice_date + timedelta(days=rng.randint(10, 35))
    num_lines = rng.randint(supplier["min_lines"], supplier["max_lines"])
    base_total = rng.uniform(*supplier["base_range"])
    if supplier.get("is_credit"):
        base_total *= -1
    line_amounts = _split_amount(base_total, num_lines, rng)
    lines = []
    for amount in line_amounts:
        vat_rate = rng.choice(supplier["vat_rates"])
        desc_template = rng.choice(supplier["concepts"])
        desc = desc_template.format(month=rng.choice(MONTHS), year=invoice_date.year)
        lines.append(
            {
                "desc": desc,
                "amount": round(amount, 2),
                "vat_rate": vat_rate,
                "vat_amount": round(amount * vat_rate / 100.0, 2),
            }
        )
    base_sum = round(sum(line["amount"] for line in lines), 2)
    vat_sum = round(sum(line["vat_amount"] for line in lines), 2)
    gross_sum = round(base_sum + vat_sum, 2)
    invoice_number = f"{supplier['category'][:3].upper()}-{invoice_date.year}-{idx:05d}"
    filename = f"factura_{idx:05d}_{supplier['category']}.pdf"
    return {
        "supplier": supplier,
        "customer": CUSTOMER,
        "invoice_number": invoice_number,
        "filename": filename,
        "invoice_date": invoice_date.isoformat(),
        "due_date": due_date.isoformat(),
        "lines": lines,
        "totals": {"base": base_sum, "vat": vat_sum, "gross": gross_sum},
        "category": supplier["category"],
    }


def _render_table_line(c: canvas.Canvas, x: float, y: float, width: float, text: str) -> None:
    c.setStrokeColor(colors.grey)
    c.rect(x, y - 6, width, 14, stroke=1, fill=0)
    c.drawString(x + 4, y, text)


def render_pdf(invoice: Dict, path: Path) -> None:
    c = canvas.Canvas(str(path), pagesize=A4)
    width, height = A4
    margin = 20 * mm
    y = height - margin
    c.setFont("Helvetica-Bold", 16)
    c.drawString(margin, y, "FACTURA")
    c.setFont("Helvetica", 10)
    c.drawString(margin + 360, y, f"Nº {invoice['invoice_number']}")
    y -= 20
    supplier = invoice["supplier"]
    c.drawString(margin, y, supplier["name"])
    y -= 12
    c.drawString(margin, y, f"NIF: {supplier['nif']}")
    y -= 12
    c.drawString(margin, y, f"Categoría: {invoice['category']}")
    y -= 20
    c.drawString(margin, y, CUSTOMER["name"])
    y -= 12
    c.drawString(margin, y, f"NIF: {CUSTOMER['nif']}")
    y -= 12
    c.drawString(margin, y, CUSTOMER["address"])
    y -= 20
    c.drawString(margin, y, f"Fecha factura: {invoice['invoice_date']}")
    y -= 12
    c.drawString(margin, y, f"Vencimiento: {invoice['due_date']}")
    y -= 20
    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin, y, "Descripción")
    c.drawString(margin + 320, y, "Base (€)")
    c.drawString(margin + 400, y, "IVA (%)")
    c.drawString(margin + 460, y, "IVA (€)")
    c.drawString(margin + 520, y, "Total (€)")
    y -= 8
    c.line(margin, y, width - margin, y)
    y -= 12
    c.setFont("Helvetica", 10)
    for line in invoice["lines"]:
        if y < 80:
            c.showPage()
            y = height - margin
        c.drawString(margin, y, line["desc"][:60])
        c.drawRightString(margin + 380, y, f"{line['amount']:.2f}")
        c.drawRightString(margin + 430, y, f"{line['vat_rate']:.0f}")
        c.drawRightString(margin + 500, y, f"{line['vat_amount']:.2f}")
        c.drawRightString(margin + 560, y, f"{line['amount'] + line['vat_amount']:.2f}")
        y -= 14
    y -= 10
    totals = invoice["totals"]
    c.setFont("Helvetica-Bold", 11)
    c.drawRightString(margin + 430, y, "Base imponible:")
    c.drawRightString(margin + 560, y, f"{totals['base']:.2f} €")
    y -= 14
    c.drawRightString(margin + 430, y, "IVA:")
    c.drawRightString(margin + 560, y, f"{totals['vat']:.2f} €")
    y -= 14
    c.drawRightString(margin + 430, y, "Total factura:")
    c.drawRightString(margin + 560, y, f"{totals['gross']:.2f} €")
    y -= 20
    c.setFont("Helvetica", 9)
    c.drawString(margin, y, f"IBAN: {CUSTOMER['iban']} · Pago domiciliado")
    c.showPage()
    c.save()


def _prepare_destination(path: Path, purge: bool) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if purge:
        for pdf in path.glob("*.pdf"):
            pdf.unlink()


def generate_samples(count: int, out_tests: Path, out_in: Path | None, seed: int, purge: bool) -> None:
    rng = random.Random(seed)
    _prepare_destination(out_tests, purge=purge)
    if out_in:
        _prepare_destination(out_in, purge=purge)
    for idx in range(1, count + 1):
        invoice = _build_invoice(idx, rng)
        target = out_tests / invoice["filename"]
        render_pdf(invoice, target)
        if out_in:
            copy_target = out_in / invoice["filename"]
            copy_target.write_bytes(target.read_bytes())
        if idx % 50 == 0 or idx == count:
            print(f"Generadas {idx}/{count} facturas…")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generador de facturas sintéticas realistas")
    parser.add_argument("--count", type=int, default=100, help="Número de facturas a generar (default: 100)")
    parser.add_argument("--out-tests", type=Path, default=DEFAULT_TESTS, help="Carpeta destino en tests/")
    parser.add_argument("--out-in", type=Path, default=DEFAULT_IN, help="Carpeta destino en IN/ para el pipeline")
    parser.add_argument("--seed", type=int, default=42, help="Semilla para la generación aleatoria")
    parser.add_argument("--no-purge", action="store_true", help="No borrar los PDFs existentes en destino")
    return parser.parse_args()


def main() -> None:  # pragma: no cover - CLI
    args = parse_args()
    generate_samples(
        count=args.count,
        out_tests=args.out_tests,
        out_in=args.out_in,
        seed=args.seed,
        purge=not args.no_purge,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
