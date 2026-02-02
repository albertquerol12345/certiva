from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List, Set

try:  # Prefer the canonical BASE_DIR from utils when available
    from src import utils

    BASE_DIR = Path(utils.BASE_DIR)
except Exception:  # pragma: no cover - fallback for standalone usage
    BASE_DIR = Path(__file__).resolve().parents[1]


IGNORED_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    ".idea",
    ".vscode",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "node_modules",
    "db",
    "OUT",
    "IN",
    "build",
    "dist",
}
ALLOWED_EXTENSIONS: Set[str] = {
    ".py",
    ".pyi",
    ".html",
    ".htm",
    ".jinja2",
    ".j2",
    ".json",
    ".xsd",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".md",
    ".rst",
    ".txt",
    ".env",
    ".sample",
    ".sh",
    ".ps1",
    ".sql",
}
SPECIAL_FILENAMES = {
    "Dockerfile",
    "docker-compose.yml",
    "requirements.txt",
    "requirements-dev.txt",
    "tenants.json",
    ".env",
    ".env.sample",
    "Makefile",
    "LICENSE",
    "README",
    "README.md",
}
SNAPSHOT_NAME_DEFAULT = "certiva_snapshot_20251114_full.txt"


def should_skip_dir(path: Path) -> bool:
    return path.name in IGNORED_DIRS


def should_include_file(path: Path) -> bool:
    name = path.name
    if name in SPECIAL_FILENAMES:
        return True
    suffix = path.suffix.lower()
    if suffix in ALLOWED_EXTENSIONS:
        return True
    if suffix == "":
        # include top-level config files without extension (e.g., Makefile) handled above
        return False
    return False


def discover_files(base: Path) -> List[Path]:
    results: List[Path] = []
    stack: List[Path] = [base]
    while stack:
        current = stack.pop()
        for child in current.iterdir():
            if child.is_dir():
                if should_skip_dir(child):
                    continue
                stack.append(child)
            elif child.is_file() and should_include_file(child):
                results.append(child)
    return sorted(results)


def write_snapshot(files: Iterable[Path], output: Path, base: Path) -> None:
    files_list = list(files)
    with output.open("w", encoding="utf-8") as fh:
        for index, file_path in enumerate(files_list):
            rel_path = file_path.relative_to(base)
            fh.write(f"/// FILE: {rel_path.as_posix()}\n")
            content = file_path.read_text(encoding="utf-8")
            fh.write(content)
            if not content.endswith("\n"):
                fh.write("\n")
            fh.write("/// END FILE\n")
            if index != len(files_list) - 1:
                fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Dump CERTIVA repository files into a single snapshot.")
    parser.add_argument(
        "--output",
        default=SNAPSHOT_NAME_DEFAULT,
        help="Nombre del archivo de snapshot (por defecto: %(default)s)",
    )
    args = parser.parse_args()
    output_path = BASE_DIR / args.output
    if output_path.exists():
        output_path.unlink()
    files = discover_files(BASE_DIR)
    write_snapshot(files, output_path, BASE_DIR)
    print(f"Snapshot written to {output_path}")


if __name__ == "__main__":
    main()
