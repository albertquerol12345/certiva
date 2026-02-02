"""Degrada PDFs limpios a versiones sucias (fotos/escaneos)."""
from __future__ import annotations

import argparse
import random
from io import BytesIO
from pathlib import Path
from typing import Iterable

try:  # pragma: no cover - optional dependency
    from pdf2image import convert_from_path  # type: ignore
except ImportError:  # pragma: no cover - fallback
    convert_from_path = None  # type: ignore
from PIL import Image, ImageEnhance, ImageFilter

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = BASE_DIR / "tests" / "golden"
DEFAULT_DEST = BASE_DIR / "tests" / "golden_dirty"


def augment_pdf(pdf_path: Path, dest: Path, rng: random.Random, dpi: int) -> Path:
    if convert_from_path is None:
        raise RuntimeError("pdf2image no está instalado; no se puede degradar PDFs")
    images = convert_from_path(str(pdf_path), dpi=dpi)
    dirty_pages = []
    for page in images:
        page = page.convert("RGB")
        angle = rng.uniform(-12, 12) if rng.random() < 0.7 else rng.uniform(-18, 18)
        rotated = page.rotate(angle, expand=1, fillcolor="white")
        blur_radius = rng.uniform(0.6, 2.0)
        blurred = rotated.filter(ImageFilter.GaussianBlur(radius=blur_radius))
        contrast_factor = rng.uniform(0.85, 1.15)
        brightness_factor = rng.uniform(0.85, 1.15)
        contrast_img = ImageEnhance.Contrast(blurred).enhance(contrast_factor)
        brightness_img = ImageEnhance.Brightness(contrast_img).enhance(brightness_factor)
        buffer = BytesIO()
        quality = rng.randint(35, 60)
        brightness_img.save(buffer, format="JPEG", quality=quality)
        buffer.seek(0)
        compressed = Image.open(BytesIO(buffer.getvalue()))
        dirty_pages.append(compressed)
    output = dest / (pdf_path.stem + "-dirty.pdf")
    dest.mkdir(parents=True, exist_ok=True)
    dirty_pages[0].save(output, format="PDF", save_all=True, append_images=dirty_pages[1:])
    return output


def augment_folder(source: Path, dest: Path, seed: int = 42, dpi: int = 200, purge: bool = True, limit: int | None = None) -> Iterable[Path]:
    if convert_from_path is None:
        raise RuntimeError("pdf2image no disponible; ejecuta `pip install pdf2image` y configura poppler")
    rng = random.Random(seed)
    dest.mkdir(parents=True, exist_ok=True)
    if purge:
        for pdf in dest.glob("*.pdf"):
            pdf.unlink()
    files = sorted(source.glob("*.pdf"))
    if limit:
        files = files[:limit]
    for pdf in files:
        yield augment_pdf(pdf, dest=dest, rng=rng, dpi=dpi)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aplica degradaciones visuales a PDFs")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="Carpeta con PDFs limpios")
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST, help="Carpeta destino para PDFs sucios")
    parser.add_argument("--seed", type=int, default=123, help="Semilla para los efectos aleatorios")
    parser.add_argument("--dpi", type=int, default=200, help="Resolución para renderizar el PDF a imagen")
    parser.add_argument("--limit", type=int, help="Número máximo de PDFs a procesar")
    parser.add_argument("--no-purge", action="store_true", help="No borrar el contenido previo del destino")
    return parser.parse_args()


def main() -> None:  # pragma: no cover
    args = parse_args()
    if convert_from_path is None:
        print("pdf2image no está disponible. Instala 'pdf2image' y poppler para usar este script.")
        return
    source = args.source
    if not source.exists():
        print(f"No existe la carpeta {source}")
        return
    pdfs = list(augment_folder(source, args.dest, seed=args.seed, dpi=args.dpi, purge=not args.no_purge, limit=args.limit))
    if not pdfs:
        print("No se encontraron PDFs de entrada")
        return
    for pdf in pdfs:
        print(f"Generado {pdf}")


if __name__ == "__main__":
    main()
