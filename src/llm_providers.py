"""LLM provider abstractions for CERTIVA."""
from __future__ import annotations

from abc import ABC, abstractmethod
import json
import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


class LLMProvider(ABC):
    provider_name = "undefined"

    def __init__(self) -> None:
        self._last_debug_payload: Optional[Dict[str, Any]] = None

    @abstractmethod
    def propose_mapping(self, invoice: Dict[str, Any]) -> Dict[str, Any]:
        """Devuelve sugerencias de cuenta/IVA/issue codes para un documento."""
        raise NotImplementedError

    def set_debug_payload(self, payload: Dict[str, Any]) -> None:
        self._last_debug_payload = payload

    def consume_debug_payload(self) -> Optional[Dict[str, Any]]:
        payload = self._last_debug_payload
        self._last_debug_payload = None
        return payload


class DummyLLMProvider(LLMProvider):
    """Provider offline basado en heurísticas deterministas."""

    provider_name = "dummy"

    HEURISTICS: List[Tuple[str, str, float, str]] = [
        ("ARREND", "621000", 21.0, "Servicios de alquiler detectados"),
        ("IBERDROLA", "628000", 21.0, "Suministros eléctricos"),
        ("ENDESA", "628000", 21.0, "Suministros eléctricos"),
        ("GOOGLE", "627000", 21.0, "Publicidad/marketing"),
        ("FACEBOOK", "627000", 21.0, "Publicidad/marketing"),
        ("AMAZON", "629000", 21.0, "Compras generalistas"),
        ("RENTA", "621000", 21.0, "Pago de rentas"),
        ("VIAJE", "624000", 21.0, "Serv. viajes"),
    ]

    def __init__(self) -> None:
        super().__init__()

    def propose_mapping(self, invoice: Dict[str, Any]) -> Dict[str, Any]:
        supplier = (invoice.get("supplier") or {}).get("name", "")
        lines = invoice.get("lines") or []
        blob = f"{supplier} " + " ".join(line.get("desc", "") for line in lines)
        upper = blob.upper()
        for token, account, iva, rationale in self.HEURISTICS:
            if token in upper:
                mapping = {
                    "account": account,
                    "iva_type": iva,
                    "confidence_llm": 0.75,
                    "rationale": rationale,
                    "issue_codes": [],
                    "provider": self.provider_name,
                    "model_used": "dummy",
                }
                self.set_debug_payload(
                    {
                        "prompt": {
                            "supplier": supplier,
                            "blob": blob,
                            "token_match": token,
                        },
                        "response_raw": {"account": account, "iva_type": iva},
                        "parsed_result": mapping,
                    }
                )
                return mapping
        mapping = {
            "account": "629000",
            "iva_type": 21.0,
            "confidence_llm": 0.5,
            "rationale": "Dummy fallback mapping",
            "issue_codes": [],
            "provider": self.provider_name,
            "model_used": "dummy",
        }
        self.set_debug_payload(
            {
                "prompt": {"supplier": supplier, "blob": blob, "token_match": None},
                "response_raw": {"account": "629000", "iva_type": 21.0},
                "parsed_result": mapping,
            }
        )
        return mapping




class _OpenAIResponder:
    """Wrapper alrededor del cliente OpenAI Responses API."""

    def __init__(self, api_key: str, api_base: str) -> None:
        try:
            from openai import OpenAI  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("Instala openai>=1.0 para usar OpenAILLMProvider") from exc
        self._client = OpenAI(api_key=api_key, base_url=api_base)
        self._schema = {
            "type": "json_schema",
            "json_schema": {
                "name": "mapping",
                "schema": {
                    "type": "object",
                    "properties": {
                        "account": {"type": "string"},
                        "iva_type": {"type": ["number", "null"]},
                        "issue_codes": {
                            "type": "array",
                            "items": {"type": "string"},
                            "default": [],
                        },
                        "rationale": {"type": "string"},
                    },
                    "required": ["account", "iva_type", "issue_codes"],
                    "additionalProperties": False,
                },
            },
        }
        self._system_prompt = (
            "Eres un asistente contable experto en el Plan General Contable español. "
            "Recibirás el JSON de una factura normalizada y debes devolver exclusivamente "
            "un JSON con los campos account (texto, cuenta 6xx/7xx), iva_type (número) e issue_codes "
            "(array de strings si detectas anomalías). No añadas texto adicional."
        )

    @staticmethod
    def _invoice_payload(invoice: Dict[str, Any]) -> Dict[str, Any]:
        lines = (invoice.get("lines") or [])[:5]
        return {
            "supplier": invoice.get("supplier"),
            "totals": invoice.get("totals"),
            "invoice": invoice.get("invoice"),
            "doc_type": (invoice.get("metadata") or {}).get("doc_type"),
            "category": (invoice.get("metadata") or {}).get("category"),
            "lines": lines,
        }

    @staticmethod
    def _extract_text_blocks(response: Any) -> str:  # pragma: no cover - structure depends on SDK
        chunks: List[str] = []
        for output in getattr(response, "output", []):
            content = getattr(output, "content", [])
            if isinstance(content, list):
                for block in content:
                    text = getattr(block, "text", None)
                    if text:
                        chunks.append(text)
                    elif isinstance(block, dict) and block.get("text"):
                        chunks.append(block["text"])
        return "".join(chunks)

    @staticmethod
    def _usage_payload(usage: Any) -> Dict[str, int]:
        if usage is None:
            return {"prompt_tokens": 0, "completion_tokens": 0}
        prompt = getattr(usage, "prompt_tokens", None)
        if prompt is None:
            prompt = getattr(usage, "input_tokens", 0)
        completion = getattr(usage, "completion_tokens", None)
        if completion is None:
            completion = getattr(usage, "output_tokens", 0)
        try:
            prompt_val = int(prompt or 0)
        except (TypeError, ValueError):
            prompt_val = 0
        try:
            completion_val = int(completion or 0)
        except (TypeError, ValueError):
            completion_val = 0
        return {"prompt_tokens": prompt_val, "completion_tokens": completion_val}

    def call(self, model_id: str, invoice: Dict[str, Any], temperature: float = 0.0) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, int]]:
        payload_dict = self._invoice_payload(invoice)
        payload = json.dumps(payload_dict, ensure_ascii=False)
        inputs = [
            {"role": "system", "content": self._system_prompt},
            {"role": "user", "content": payload},
        ]
        delays = [0.0, 0.8, 2.0]
        last_error: Optional[Exception] = None
        for attempt, delay in enumerate(delays, start=1):
            try:
                start = time.perf_counter()
                response = self._client.responses.create(
                    model=model_id,
                    max_output_tokens=256,
                    input=inputs,
                )
                raw_text = self._extract_text_blocks(response)
                data = json.loads(raw_text or "{}")
                data.setdefault("issue_codes", [])
                data["duration_ms"] = int((time.perf_counter() - start) * 1000)
                debug = {
                    "system_prompt": self._system_prompt,
                    "prompt": payload_dict,
                    "response_text": raw_text,
                }
                usage = self._usage_payload(getattr(response, "usage", None))
                return data, debug, usage
            except Exception as exc:  # pragma: no cover
                last_error = exc
                logger.warning("LLM OpenAI error (%s): %s", model_id, exc)
                if attempt < len(delays):
                    time.sleep(delay)
                    continue
                raise RuntimeError(str(exc)) from exc
        raise RuntimeError(str(last_error))


class OpenAILLMProvider(LLMProvider):
    """LLM simple basado en un único modelo OpenAI."""

    provider_name = "openai"

    def __init__(
        self,
        api_key: Optional[str],
        model: str,
        api_base: str,
        responder: Optional[_OpenAIResponder] = None,
        pricing: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> None:
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY no configurada")
        if not model:
            raise RuntimeError("OPENAI_MODEL no configurado")
        super().__init__()
        self.model = model
        self._responder = responder or _OpenAIResponder(api_key, api_base)
        self.pricing = pricing or {}

    def _finalize(self, mapping: Dict[str, Any], model_used: str) -> Dict[str, Any]:
        issues = mapping.get("issue_codes") or []
        mapping["issue_codes"] = list(issues)
        mapping["provider"] = self.provider_name
        mapping["model_used"] = model_used
        mapping.setdefault("confidence_llm", 0.8)
        mapping.setdefault("prompt_tokens", 0)
        mapping.setdefault("completion_tokens", 0)
        mapping.setdefault("cost_eur", 0.0)
        return mapping

    def _cost_for(self, label: str, prompt_tokens: int, completion_tokens: int) -> float:
        rate = self.pricing.get(label) or {"in": 0.0, "out": 0.0}
        in_rate = float(rate.get("in", 0.0))
        out_rate = float(rate.get("out", 0.0))
        return round((prompt_tokens / 1_000_000) * in_rate + (completion_tokens / 1_000_000) * out_rate, 6)

    def propose_mapping(self, invoice: Dict[str, Any]) -> Dict[str, Any]:
        try:
            mapping, debug, usage = self._responder.call(self.model, invoice)
            finalized = self._finalize(mapping, "mini")
            prompt_tokens = int(usage.get("prompt_tokens", 0))
            completion_tokens = int(usage.get("completion_tokens", 0))
            finalized["prompt_tokens"] = prompt_tokens
            finalized["completion_tokens"] = completion_tokens
            finalized["cost_eur"] = self._cost_for("mini", prompt_tokens, completion_tokens)
            self.set_debug_payload(
                {
                    "prompt": debug.get("prompt"),
                    "response_raw": debug.get("response_text"),
                    "parsed_result": finalized,
                }
            )
            return finalized
        except RuntimeError as exc:
            logger.warning("LLM OpenAI error definitivo: %s", exc)
            return {
                "account": "",
                "iva_type": None,
                "confidence_llm": 0.0,
                "rationale": str(exc),
                "issue_codes": ["LLM_ERROR"],
                "provider": self.provider_name,
                "model_used": "mini",
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "cost_eur": 0.0,
            }


class DualOpenAILLMProvider(LLMProvider):
    """Cascada mini → premium para casos complejos."""

    provider_name = "openai-dual"
    HARD_ISSUE_CODES = {"LLM_ERROR", "MAPPING_AMBIGUOUS"}
    PREMIUM_CATEGORIES = {
        "intracomunitaria",
        "ventas_intracom",
        "ventas_abono",
        "nota_credito",
        "abono",
    }

    def __init__(
        self,
        api_key: Optional[str],
        api_base: str,
        model_mini: str,
        model_premium: str,
        threshold_gross: float,
        responder: Optional[_OpenAIResponder] = None,
        pricing: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> None:
        super().__init__()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY no configurada")
        self.model_mini = model_mini
        self.model_premium = model_premium or model_mini
        self.threshold_gross = threshold_gross
        self._responder = responder or _OpenAIResponder(api_key, api_base)
        self.pricing = pricing or {}

    def _call_model(self, model_id: str, invoice: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, int]]:
        mapping, debug, usage = self._responder.call(model_id, invoice)
        mapping.setdefault("issue_codes", [])
        mapping["provider"] = self.provider_name
        return mapping, debug, usage

    def _should_escalate(self, mapping: Dict[str, Any], invoice: Dict[str, Any]) -> bool:
        issue_codes = set(mapping.get("issue_codes") or [])
        if issue_codes.intersection(self.HARD_ISSUE_CODES):
            return True
        metadata = invoice.get("metadata") or {}
        category = (metadata.get("category") or "").lower()
        doc_type = (metadata.get("doc_type") or "").lower()
        if category in self.PREMIUM_CATEGORIES or doc_type in {"sales_credit_note", "sales_intracom"}:
            return True
        gross = float((invoice.get("totals") or {}).get("gross") or 0.0)
        threshold = self.threshold_gross
        override = invoice.get("_llm_threshold_override")
        if override is None:
            override = (metadata.get("llm_threshold_override") if isinstance(metadata, dict) else None)
        try:
            if override is not None:
                threshold = float(override)
        except (TypeError, ValueError):
            threshold = self.threshold_gross
        return gross >= threshold

    @staticmethod
    def _merge_issue_codes(*groups: Iterable[str]) -> List[str]:
        merged: List[str] = []
        seen = set()
        for group in groups:
            for code in group or []:
                if code not in seen:
                    seen.add(code)
                    merged.append(code)
        return merged

    def _cost_for(self, label: str, prompt_tokens: int, completion_tokens: int) -> float:
        rate = self.pricing.get(label) or self.pricing.get("mini", {})
        in_rate = float(rate.get("in", 0.0))
        out_rate = float(rate.get("out", 0.0))
        cost = (prompt_tokens / 1_000_000) * in_rate + (completion_tokens / 1_000_000) * out_rate
        return round(cost, 6)

    def _finalize(self, mapping: Dict[str, Any], label: str) -> Dict[str, Any]:
        mapping["model_used"] = label
        mapping.setdefault("confidence_llm", 0.85 if label == "premium" else 0.8)
        mapping.setdefault("prompt_tokens", 0)
        mapping.setdefault("completion_tokens", 0)
        mapping.setdefault("cost_eur", 0.0)
        return mapping

    def propose_mapping(self, invoice: Dict[str, Any]) -> Dict[str, Any]:
        stage_debug: Dict[str, Any] = {}
        try:
            mini_mapping, mini_debug, mini_usage = self._call_model(self.model_mini, invoice)
            stage_debug["mini"] = {
                "prompt": mini_debug.get("prompt"),
                "response_raw": mini_debug.get("response_text"),
                "parsed_result": mini_mapping,
            }
            mini_prompt = int(mini_usage.get("prompt_tokens", 0))
            mini_completion = int(mini_usage.get("completion_tokens", 0))
            mini_cost = self._cost_for("mini", mini_prompt, mini_completion)
            mini_mapping["prompt_tokens"] = mini_prompt
            mini_mapping["completion_tokens"] = mini_completion
            mini_mapping["cost_eur"] = mini_cost
        except RuntimeError as exc:
            logger.warning("LLM mini falló: %s", exc)
            return {
                "account": "",
                "iva_type": None,
                "confidence_llm": 0.0,
                "rationale": str(exc),
                "issue_codes": ["LLM_ERROR"],
                "provider": self.provider_name,
                "model_used": "mini",
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "cost_eur": 0.0,
            }

        if not self._should_escalate(mini_mapping, invoice):
            finalized = self._finalize(mini_mapping, "mini")
            self.set_debug_payload(stage_debug.get("mini", {}))
            return finalized

        try:
            premium_mapping, premium_debug, premium_usage = self._call_model(self.model_premium, invoice)
            premium_mapping["issue_codes"] = self._merge_issue_codes(
                mini_mapping.get("issue_codes"),
                premium_mapping.get("issue_codes"),
            )
            total_prompt = int(mini_mapping.get("prompt_tokens", 0)) + int(premium_usage.get("prompt_tokens", 0))
            total_completion = int(mini_mapping.get("completion_tokens", 0)) + int(premium_usage.get("completion_tokens", 0))
            premium_cost = self._cost_for("premium", int(premium_usage.get("prompt_tokens", 0)), int(premium_usage.get("completion_tokens", 0)))
            total_cost = float(mini_mapping.get("cost_eur", 0.0)) + premium_cost
            premium_mapping["prompt_tokens"] = total_prompt
            premium_mapping["completion_tokens"] = total_completion
            premium_mapping["cost_eur"] = total_cost
            finalized = self._finalize(premium_mapping, "premium")
            stage_debug["premium"] = {
                "prompt": premium_debug.get("prompt"),
                "response_raw": premium_debug.get("response_text"),
                "parsed_result": finalized,
            }
            self.set_debug_payload(stage_debug)
            return finalized
        except RuntimeError as exc:  # pragma: no cover - premium fallback
            logger.warning("LLM premium falló: %s", exc)
            fallback = dict(mini_mapping)
            fallback["issue_codes"] = self._merge_issue_codes(mini_mapping.get("issue_codes"), ["LLM_PREMIUM_ERROR"])
            finalized = self._finalize(fallback, "mini")
            stage_debug["premium_error"] = {"error": str(exc)}
            self.set_debug_payload(stage_debug)
            return finalized
