"""Asistente RAG normativo (PGC, IVA, SII) para CERTIVA."""
from __future__ import annotations

import argparse
import json
import logging
from difflib import SequenceMatcher
from pathlib import Path
from typing import List, Optional, Tuple

from . import llm_router, utils
from .rules_engine import ISSUE_MESSAGES  # reutilizamos etiquetas humanas

logger = logging.getLogger(__name__)

CORPUS_DIR = utils.BASE_DIR / "data" / "normativa"
_CORPUS_CACHE: List[Tuple[str, str]] = []


def _load_corpus() -> List[Tuple[str, str]]:
    global _CORPUS_CACHE
    if _CORPUS_CACHE:
        return _CORPUS_CACHE
    chunks: List[Tuple[str, str]] = []
    if not CORPUS_DIR.exists():
        logger.warning("No existe el directorio de normativa %s", CORPUS_DIR)
        return []
    for path in sorted(CORPUS_DIR.glob("*.md")):
        content = path.read_text(encoding="utf-8")
        sections = content.split("## ")
        if sections:
            # La primera sección puede ser el encabezado general
            chunks.append((path.stem, sections[0].strip()))
            for section in sections[1:]:
                lines = section.strip().splitlines()
                if not lines:
                    continue
                title = lines[0].strip()
                body = "\n".join(lines[1:]).strip()
                chunks.append((f"{path.stem} / {title}", body))
        else:
            chunks.append((path.stem, content))
    _CORPUS_CACHE = chunks
    return chunks


def _pick_relevant_fragments(question: str, max_results: int = 3) -> List[str]:
    question_lower = question.lower()
    fragments = []
    for title, body in _load_corpus():
        text = body.lower()
        score = 0
        for token in question_lower.split():
            if token and token in text:
                score += 1
        ratio = SequenceMatcher(None, question_lower, text[: min(len(text), 400)]).ratio()
        score += ratio * 5
        fragments.append((score, title, body))
    fragments.sort(key=lambda item: item[0], reverse=True)
    return [f"### {title}\n{body}" for _, title, body in fragments[:max_results] if body.strip()]


def answer_normative_question(question: str, tenant: Optional[str] = None, user: Optional[str] = None) -> str:
    fragments = _pick_relevant_fragments(question)
    if not fragments:
        fragments = ["No se encontró normativa específica, responde con criterios generales de contabilidad española."]
    context = "\n\n".join(fragments)
    system_prompt = (
        "Eres un asesor contable/fiscal experto en normativa española. "
        "Responde en castellano claro citando la sección del contexto cuando sea posible. "
        "No inventes información fuera del corpus."
    )
    user_prompt = f"Pregunta: {question}"
    return llm_router.call_llm(
        llm_router.LLMTask.RAG_NORMATIVO,
        system_prompt,
        user_prompt,
        context=context,
        tenant=tenant,
        user=user,
    )


def explain_doc_issues(doc_id: str, tenant: Optional[str] = None, user: Optional[str] = None) -> str:
    doc = utils.get_doc(doc_id)
    if not doc:
        return f"No encuentro el documento {doc_id} en la base de datos."
    issues = []
    if doc["issues"]:
        try:
            issues = json.loads(doc["issues"])
        except json.JSONDecodeError:
            issues = []
    issues_human = [ISSUE_MESSAGES.get(code, code) for code in issues]
    normalized = utils.read_json(utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json")
    metadata = normalized.get("metadata") or {}
    category = metadata.get("category") or "desconocida"
    flow = metadata.get("flow") or ("AR" if (doc["doc_type"] or "").startswith("sales") else "AP")
    question = (
        f"Tengo una factura {doc_id} (tenant {doc['tenant']}) de tipo {flow} y categoría {category}. "
        f"Tiene los siguientes issues detectados: {', '.join(issues_human) or 'ninguno'}. "
        "Explica por qué pueden aparecer estos issues según la normativa y qué debe revisar el equipo."
    )
    return answer_normative_question(question, tenant=tenant or doc["tenant"], user=user)


def explain_entry_choice(doc_id: str, tenant: Optional[str] = None, user: Optional[str] = None) -> str:
    entry_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.entry.json"
    if not entry_path.exists():
        return f"No se ha generado el asiento para {doc_id}."
    entry = utils.read_json(entry_path)
    lines = entry.get("lines", [])
    resumen = []
    for line in lines[:10]:
        resumen.append(f"{line.get('account')} → Debe {line.get('debit')} / Haber {line.get('credit')}")
    doc = utils.get_doc(doc_id)
    flow = entry.get("flow") or ("AR" if (doc["doc_type"] or "").startswith("sales") else "AP")
    question = (
        f"Explica de manera sencilla por qué la factura {doc_id} ({flow}) se ha imputado a las cuentas siguientes:\n"
        + "\n".join(resumen)
        + "\nJustifica cada cuenta con referencias al PGC."
    )
    return answer_normative_question(question, tenant=tenant or doc.get("tenant"), user=user)


def _cmd_ask(args: argparse.Namespace) -> None:
    print(answer_normative_question(args.question))


def _cmd_explain_doc(args: argparse.Namespace) -> None:
    print(explain_doc_issues(args.doc_id))


def _cmd_explain_entry(args: argparse.Namespace) -> None:
    print(explain_entry_choice(args.doc_id))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Asistente RAG normativo CERTIVA")
    sub = parser.add_subparsers(dest="command", required=True)

    ask = sub.add_parser("ask", help="Preguntar sobre normativa contable/IVA")
    ask.add_argument("--question", required=True)
    ask.set_defaults(func=_cmd_ask)

    doc = sub.add_parser("explain-doc", help="Explicar issues detectados en un documento")
    doc.add_argument("--doc-id", required=True)
    doc.set_defaults(func=_cmd_explain_doc)

    entry = sub.add_parser("explain-entry", help="Explicar por qué se han usado ciertas cuentas en un doc")
    entry.add_argument("--doc-id", required=True)
    entry.set_defaults(func=_cmd_explain_entry)
    return parser


def main() -> None:
    utils.configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
