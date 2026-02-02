"""
Purgar OUT/ y subcarpetas antiguas según OUT_RETENTION_DAYS.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from src import utils, config


def main() -> None:
    utils.configure_logging()
    parser = argparse.ArgumentParser(description="Purgar archivos antiguos en OUT/")
    parser.add_argument("--days", type=int, default=config.settings.out_retention_days, help="Antigüedad máxima en días")
    parser.add_argument(
        "--paths",
        nargs="*",
        type=Path,
        default=[
            utils.BASE_DIR / "OUT" / "json",
            utils.BASE_DIR / "OUT" / "csv",
            utils.BASE_DIR / "OUT" / "logs",
            utils.BASE_DIR / "OUT" / "debug",
            utils.BASE_DIR / "IN" / "archivado",
        ],
        help="Rutas a purgar",
    )
    args = parser.parse_args()
    removed = utils.delete_old_files(args.paths, args.days)
    print(f"Eliminados {removed} archivos con más de {args.days} días.")


if __name__ == "__main__":
    main()
