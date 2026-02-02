"""Utilities to remove PII patterns from text."""
from __future__ import annotations

import re
from typing import Iterable, Tuple

PatternPair = Tuple[str, str]

PII_PATTERNS_BASE: Iterable[PatternPair] = (
    (r"\bES[A-Z0-9]{9,12}\b", "[VAT_ID]"),
    (r"\b[A-Z]{1}-?\d{7}[A-Z]\b", "[DOC_ID]"),
    (r"\b\d{8}[A-Z]\b", "[DOC_ID]"),
)

PII_PATTERNS_STRICT: Iterable[PatternPair] = (
    (r"\b[A-Z]{2}\d{20,32}\b", "[IBAN]"),
    (r"\b\d{16}\b", "[CARD]"),
)

NAME_TOKEN_PATTERN = re.compile(
    r"(?i)\b(cliente|proveedor|customer|supplier|counterparty)\b\s*[:=]\s*(?P<name>[A-Za-zÁÉÍÓÚÜÑñ .'-]+?)(?=\b(con|with|y|and)\b|[0-9]|$)"
)


def scrub_pii(text: str, *, strict: bool = False, enabled: bool = True) -> str:
    """Return a version of text with PII placeholders when enabled."""
    if text is None:
        return ""
    if not enabled:
        return text
    scrubbed = text
    for pattern, replacement in PII_PATTERNS_BASE:
        scrubbed = re.sub(pattern, replacement, scrubbed, flags=re.IGNORECASE)
    if strict:
        for pattern, replacement in PII_PATTERNS_STRICT:
            scrubbed = re.sub(pattern, replacement, scrubbed, flags=re.IGNORECASE)
        scrubbed = NAME_TOKEN_PATTERN.sub(
            lambda match: match.group(0).replace(match.group("name"), "[NOMBRE]", 1),
            scrubbed,
        )
    scrubbed = re.sub(r"\b\d{8,}\b", "[NUM]", scrubbed)
    return scrubbed
