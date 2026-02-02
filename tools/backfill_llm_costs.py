"""
Backfill de cost_eur en llm_calls usando tokens y tarifas de config.
"""
from __future__ import annotations

import argparse

from src import utils


def backfill(dry_run: bool = False) -> int:
    updated = 0
    with utils.get_connection() as conn:
        rows = conn.execute(
            "SELECT ROWID as rid, model, prompt_tokens, completion_tokens FROM llm_calls WHERE cost_eur IS NULL OR cost_eur = 0"
        ).fetchall()
        for row in rows:
            cost = utils.compute_llm_cost(row["model"] or "", row["prompt_tokens"] or 0.0, row["completion_tokens"] or 0.0)
            if dry_run:
                continue
            conn.execute("UPDATE llm_calls SET cost_eur = ? WHERE ROWID = ?", (cost, row["rid"]))
            updated += 1
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill cost_eur en llm_calls")
    parser.add_argument("--dry-run", action="store_true", help="No escribe, sólo reporta")
    args = parser.parse_args()
    utils.configure_logging()
    count = backfill(dry_run=args.dry_run)
    msg = f"{count} filas actualizadas" if not args.dry_run else f"{count} filas detectarían update (dry-run)"
    print(msg)


if __name__ == "__main__":
    main()
