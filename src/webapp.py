from __future__ import annotations

from datetime import date
import secrets
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from fastapi import BackgroundTasks, Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from . import (
    auth,
    config,
    efactura_payloads,
    explain_reports,
    facturae_export,
    health,
    hitl_service,
    jobs,
    metrics,
    pipeline,
    rag_normativo,
    reports as reports_module,
    rules_engine,
    sii_export,
    utils,
)
from .config import BASE_DIR, settings
from . import bank_matcher

app = FastAPI(title="CERTIVA HITL")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
ALLOWED_ROLES = {"admin", "operator", "viewer"}

if not settings.web_session_secret or settings.web_session_secret == "change-this":
    raise RuntimeError("WEB_SESSION_SECRET debe configurarse con un valor seguro (no uses 'change-this').")

same_site = settings.session_cookie_same_site.lower()
if same_site not in {"lax", "strict", "none"}:
    same_site = "lax"

app.add_middleware(
    SessionMiddleware,
    secret_key=settings.web_session_secret,
    session_cookie="certiva_session",
    https_only=settings.session_cookie_secure,
    same_site=same_site,
    max_age=settings.session_cookie_max_age,
)


@app.middleware("http")
async def security_and_logging_middleware(request: Request, call_next):
    utils.configure_logging()
    response = await call_next(request)
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    if settings.session_cookie_secure:
        response.headers.setdefault("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:",
    )
    return response


@app.middleware("http")
async def csrf_json_guard(request: Request, call_next):
    """Defensa extra: obliga X-CSRF-Token en peticiones JSON mutantes."""
    if request.method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            session_token = request.session.get("_csrf_token")
            header_token = request.headers.get("x-csrf-token")
            if not session_token or not header_token or not secrets.compare_digest(str(session_token), str(header_token)):
                return Response("CSRF token inválido o ausente", status_code=400)
    return await call_next(request)


def _context(request: Request, extra: Dict[str, Any]) -> Dict[str, Any]:
    context = {"request": request, "csrf_token": auth.get_csrf_token(request)}
    context.update(extra)
    return context


def _render(request: Request, template_name: str, context: Dict[str, Any], status_code: int = 200) -> Response:
    """Compatibilidad con nuevos warnings de Starlette (request como primer arg)."""
    return templates.TemplateResponse(request=request, name=template_name, context=context, status_code=status_code)


def _redirect(path: str, message: Optional[str] = None) -> RedirectResponse:
    if message:
        connector = "&" if "?" in path else "?"
        path = f"{path}{connector}msg={quote_plus(message)}"
    return RedirectResponse(path, status_code=303)


def _prometheus_snapshot() -> str:
    stats_snapshot = metrics.gather_stats(tenant=None)
    preflight_snapshot = metrics.gather_preflight(tenant=None)
    lines = []

    def add_metric(name: str, value: float, help_text: str) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        lines.append(f"{name} {value}")

    docs_total = stats_snapshot.get("docs_total", 0)
    docs_posted = stats_snapshot.get("posted", 0)
    autop_ratio = (stats_snapshot.get("auto_post_pct", 0.0) or 0.0) / 100.0
    add_metric("certiva_docs_total", docs_total, "Total de documentos conocidos por CERTIVA")
    add_metric("certiva_docs_posted", docs_posted, "Documentos publicados en ERP")
    add_metric("certiva_docs_autopost_ratio", autop_ratio, "Ratio de auto-post (0-1)")

    bank = stats_snapshot.get("bank") or {}
    add_metric("certiva_bank_docs_fully", bank.get("docs_fully", 0), "Facturas totalmente conciliadas")
    add_metric("certiva_bank_docs_partial", bank.get("docs_partial", 0), "Facturas parcialmente conciliadas")
    add_metric("certiva_bank_docs_unmatched", bank.get("docs_unmatched", 0), "Facturas sin conciliación")

    llm_stats = stats_snapshot.get("llm_stats") or {}
    add_metric("certiva_llm_calls_total", llm_stats.get("total_calls", 0), "Número total de llamadas LLM registradas")
    add_metric("certiva_llm_calls_error_total", llm_stats.get("errors", 0), "Número de llamadas LLM con error")
    add_metric("certiva_llm_avg_latency_ms", llm_stats.get("avg_latency_ms", 0.0) or 0.0, "Latencia media en ms de las llamadas LLM")
    add_metric("certiva_llm_cost_total_eur", llm_stats.get("cost_total_eur", 0.0) or 0.0, "Coste total estimado LLM (€)")
    add_metric("certiva_llm_cost_today_eur", llm_stats.get("cost_today_eur", 0.0) or 0.0, "Coste diario estimado LLM (€)")

    add_metric(
        "certiva_preflight_duplicates",
        preflight_snapshot.get("duplicates", 0),
        "Documentos marcados como duplicados en el checklist pre-SII",
    )
    add_metric("certiva_review_queue_size", preflight_snapshot.get("total", 0), "Documentos pendientes en la cola HITL")
    pages = stats_snapshot.get("pages") or {}
    add_metric("certiva_pages_total", pages.get("total_pages", 0), "Páginas contabilizadas en PDFs")
    add_metric("certiva_pages_missing_docs", pages.get("missing_page_docs", 0), "Docs sin conteo de páginas")
    add_metric("certiva_pages_zero_docs", pages.get("zero_page_docs", 0), "Docs con 0 páginas")
    batch_warn = stats_snapshot.get("batch_warnings") or {}
    if hasattr(batch_warn, "items"):
        for code, count in batch_warn.items():
            metric_name = f"certiva_batch_warning_{str(code).lower()}"
            add_metric(metric_name, count, f"Conteo de warnings de batch: {code}")
    return "\n".join(lines) + "\n"


@app.get("/")
async def dashboard(request: Request, user=Depends(auth.require_user)):
    tenant = auth.current_tenant(request, user)
    stats = metrics.gather_stats(tenant=tenant)
    preflight = metrics.gather_preflight(tenant=tenant)
    issue_counts = [
        (code, count, rules_engine.ISSUE_MESSAGES.get(code, code))
        for code, count in preflight["issue_counts"].most_common()
    ]
    review_summary = hitl_service.summarize_review_queue(tenant=tenant)
    bank_stats = stats.get("bank") or {}
    for key in ("docs_total", "docs_matched", "docs_unmatched", "tx_total", "tx_matched", "tx_unmatched"):
        bank_stats.setdefault(key, 0)
    ar_stats = stats.get("ar_summary") or {}
    jobs = stats.get("jobs") or []
    pnl_summary = stats.get("pnl_summary") or {}
    vat_summary = stats.get("vat_summary") or {}
    aging_summary = stats.get("aging_summary") or {}
    cashflow_summary = stats.get("cashflow_summary") or {}
    today = date.today()
    first_day = today.replace(day=1)
    return _render(
        request,
        "dashboard.html",
        _context(
            request,
            {
                "stats": stats,
                "preflight": {
                    "total": preflight["total"],
                    "status_counts": preflight["status_counts"].most_common(),
                    "issue_counts": issue_counts,
                    "duplicates": preflight["duplicates"],
                },
                "bank": bank_stats,
                "ar": ar_stats,
                "review_summary": review_summary,
                "jobs": jobs,
                "pnl_summary": pnl_summary,
                "vat_summary": vat_summary,
                "aging_summary": aging_summary,
                "cashflow_summary": cashflow_summary,
                "current_period": {
                    "from": first_day.isoformat(),
                    "to": today.isoformat(),
                    "months": 3,
                },
                "active_tenant": tenant,
                "user": user,
            },
        ),
    )

@app.get("/review")
async def review_list(request: Request, user=Depends(auth.require_user)):
    doc_type = request.query_params.get("doc_type")
    issue_filter = request.query_params.get("issue")
    tenant = auth.current_tenant(request, user)
    try:
        page = int(request.query_params.get("page", "1"))
    except ValueError:
        page = 1
    page = max(page, 1)
    page_size = settings.hitl_page_size
    offset = (page - 1) * page_size
    rows = hitl_service.fetch_review_items(
        limit=page_size + 1,
        offset=offset,
        doc_type_prefix=doc_type,
        tenant=tenant,
        issue_filter=issue_filter,
    )
    has_next = len(rows) > page_size
    if has_next:
        rows = rows[:page_size]
    message = request.query_params.get("msg")
    return _render(
        request,
        "review_list.html",
        _context(
            request,
            {
                "items": rows,
                "doc_type_filter": doc_type or "",
                "issue_filter": issue_filter or "",
                "user": user,
                "page": page,
                "has_next": has_next,
                "has_prev": page > 1,
                "active_tenant": tenant,
                "message": message,
            },
        ),
    )


@app.get("/review/summary")
async def review_summary(request: Request, user=Depends(auth.require_user)):
    tenant = auth.current_tenant(request, user)
    summary = hitl_service.summarize_review_queue(tenant=tenant)
    return _render(
        request,
        "review_summary.html",
        _context(
            request,
            {
                "summary": summary,
                "issue_messages": rules_engine.ISSUE_MESSAGES,
                "active_tenant": tenant,
                "user": user,
            },
        ),
    )


@app.get("/review/quick")
async def review_quick(request: Request, user=Depends(auth.require_user)):
    tenant = auth.current_tenant(request, user)
    issue_filter = request.query_params.get("issue")
    items = hitl_service.fetch_review_items(limit=15, offset=0, tenant=tenant, sort_by_issues=True, issue_filter=issue_filter)
    return _render(
        request,
        "review_quick.html",
        _context(
            request,
            {
                "items": items,
                "issue_filter": issue_filter or "",
                "active_tenant": tenant,
                "user": user,
            },
        ),
    )


@app.get("/conciliacion")
async def conciliacion_view(request: Request, user=Depends(auth.require_user)):
    tenant = auth.current_tenant(request, user)
    bank_stats = bank_matcher.gather_bank_stats(tenant=tenant, include_rows=True)
    unmatched = bank_stats.get("unmatched") or []
    matches = bank_stats.get("matches") or []
    message = request.query_params.get("msg")
    return _render(
        request,
        "conciliacion.html",
        _context(
            request,
            {
                "unmatched": unmatched,
                "matches": matches,
                "active_tenant": tenant,
                "user": user,
                "message": message,
            },
        ),
    )


@app.post("/conciliacion/clear")
async def conciliacion_clear(
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    tx_id: str = Form(...),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant = auth.current_tenant(request, user)
    bank_matcher.clear_match(tx_id)
    return _redirect("/conciliacion", "Match eliminado")


@app.post("/conciliacion/force")
async def conciliacion_force(
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    tx_id: str = Form(...),
    doc_id: str = Form(""),
    amount: Optional[float] = Form(None),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant = auth.current_tenant(request, user)
    tx_id = tx_id.strip()
    doc_id = doc_id.strip()
    if not tx_id:
        return _redirect("/conciliacion", "tx_id requerido")
    amount_val = amount if amount is not None else None
    try:
        bank_matcher.override_match(tx_id=tx_id, doc_id=doc_id or None, amount=amount_val)
    except ValueError as exc:
        return _redirect("/conciliacion", f"Error: {exc}")
    return _redirect("/conciliacion", "Match forzado")


@app.post("/review/bulk")
async def review_bulk_action(
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    action: str = Form(...),
    doc_ids: Optional[List[str]] = Form(None),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant = auth.current_tenant(request, user)
    ids = [doc_id.strip() for doc_id in (doc_ids or []) if doc_id and doc_id.strip()]
    if not ids:
        return _redirect("/review", "Selecciona al menos un documento.")
    valid_actions = {"accept", "duplicate", "reprocess"}
    if action not in valid_actions:
        return _redirect("/review", "Acción masiva no soportada.")
    for doc_id in ids:
        try:
            if action == "accept":
                detail = hitl_service.get_review_detail(doc_id, tenant=tenant)
                hitl_service.accept_doc(
                    doc_id,
                    actor=f"web:{user['username']}",
                    learn_rule=False,
                    apply_to_similar=False,
                    suggestion=detail.get("suggestion"),
                    tenant=tenant,
                )
            elif action == "duplicate":
                hitl_service.mark_duplicate(doc_id, actor=f"web:{user['username']}", tenant=tenant)
            elif action == "reprocess":
                hitl_service.reprocess_doc(doc_id, actor=f"web:{user['username']}", tenant=tenant)
        except ValueError:
            continue
    return _redirect("/review", "Acción aplicada sobre la selección.")


@app.get("/review/{doc_id}")
async def review_detail(doc_id: str, request: Request, user=Depends(auth.require_user)):
    tenant = auth.current_tenant(request, user)
    try:
        detail = hitl_service.get_review_detail(doc_id, tenant=tenant)
    except ValueError:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    return _render(
        request,
        "review_detail.html",
        _context(request, {"detail": detail, "user": user, "active_tenant": tenant}),
    )


@app.post("/review/{doc_id}/accept")
async def accept_doc(
    doc_id: str,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    learn_rule: bool = Form(False),
    apply_to_similar: bool = Form(False),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant = auth.current_tenant(request, user)
    try:
        detail = hitl_service.get_review_detail(doc_id, tenant=tenant)
    except ValueError:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    hitl_service.accept_doc(
        doc_id,
        actor=f"web:{user['username']}",
        learn_rule=learn_rule,
        apply_to_similar=apply_to_similar,
        suggestion=detail.get("suggestion"),
        tenant=tenant,
    )
    return RedirectResponse(url=f"/review/{doc_id}", status_code=303)


@app.post("/review/{doc_id}/edit")
async def edit_doc(
    doc_id: str,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    account: str = Form(...),
    iva_rate: float = Form(...),
    apply_to_similar: bool = Form(False),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant = auth.current_tenant(request, user)
    try:
        hitl_service.edit_doc(
            doc_id,
            account=account,
            iva_rate=iva_rate,
            actor=f"web:{user['username']}",
            apply_to_similar=apply_to_similar,
            tenant=tenant,
        )
    except ValueError:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    return RedirectResponse(url=f"/review/{doc_id}", status_code=303)


@app.post("/review/{doc_id}/duplicate")
async def duplicate_doc(
    doc_id: str,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant = auth.current_tenant(request, user)
    try:
        hitl_service.mark_duplicate(doc_id, actor=f"web:{user['username']}", tenant=tenant)
    except ValueError:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    return RedirectResponse(url="/review", status_code=303)


@app.post("/review/{doc_id}/reprocess")
async def reprocess_doc(
    doc_id: str,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant = auth.current_tenant(request, user)
    try:
        hitl_service.reprocess_doc(doc_id, actor=f"web:{user['username']}", tenant=tenant)
    except ValueError:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    return RedirectResponse(url=f"/review/{doc_id}", status_code=303)


@app.post("/review/{doc_id}/recon/clear")
async def clear_reconciliation(
    doc_id: str,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    include_manual: bool = Form(False),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant = auth.current_tenant(request, user)
    try:
        hitl_service.clear_reconciliation(
            doc_id,
            actor=f"web:{user['username']}",
            include_manual=include_manual,
            tenant=tenant,
        )
    except ValueError:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    return RedirectResponse(url=f"/review/{doc_id}", status_code=303)


@app.get("/login")
async def login_form(request: Request):
    if auth.current_user(request):
        return RedirectResponse("/", status_code=303)
    return _render(request, "login.html", _context(request, {"error": None}))


@app.post("/login")
async def login(
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    username: str = Form(...),
    password: str = Form(...),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    username = username.strip()
    client_ip = request.client.host if request.client else "unknown"
    recent_failures = utils.failed_attempts_since(username, settings.auth_lock_minutes)
    if recent_failures >= settings.auth_max_fails:
        utils.record_login_attempt(username, client_ip, False)
        return _render(
            request,
            "login.html",
            _context(request, {"error": "Demasiados intentos, inténtalo más tarde."}),
            status_code=400,
        )
    user_row = utils.get_user_by_username(username)
    if not user_row or not user_row["is_active"] or not auth.verify_password(password, user_row["password_hash"]):
        utils.record_login_attempt(username, client_ip, False)
        return _render(
            request,
            "login.html",
            _context(request, {"error": "Credenciales inválidas"}),
            status_code=400,
        )
    utils.record_login_attempt(username, client_ip, True)
    auth.login_user(request, user_row["username"])
    return RedirectResponse("/", status_code=303)


@app.post("/logout")
async def logout(request: Request, csrf_token: str = Form(..., alias="_csrf_token")):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    auth.logout_user(request)
    return RedirectResponse("/login", status_code=303)


@app.get("/admin/jobs")
async def admin_jobs(request: Request, user=Depends(auth.require_admin)):
    job_rows = utils.list_jobs()
    message = request.query_params.get("msg")
    return _render(request, "admin_jobs.html", _context(request, {"jobs": job_rows, "message": message, "user": user}))


@app.post("/admin/jobs/{job_id}/enable")
async def admin_job_enable(
    job_id: int,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    utils.set_job_enabled(job_id, True)
    return _redirect("/admin/jobs", "Job habilitado")


@app.post("/admin/jobs/{job_id}/disable")
async def admin_job_disable(
    job_id: int,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    utils.set_job_enabled(job_id, False)
    return _redirect("/admin/jobs", "Job deshabilitado")


@app.post("/admin/jobs/{job_id}/run")
async def admin_job_run(
    job_id: int,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    job = utils.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no encontrado")
    try:
        jobs.run_job(job)
        utils.record_job_run(job_id, "success")
        msg = "Job ejecutado correctamente"
    except Exception as exc:  # pragma: no cover - operador informado en UI
        utils.record_job_run(job_id, "error", str(exc))
        msg = f"Error ejecutando job: {exc}"
    return _redirect("/admin/jobs", msg)


@app.post("/admin/jobs/{job_id}/delete")
async def admin_job_delete(
    job_id: int,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    utils.job_delete(job_id)
    return _redirect("/admin/jobs", "Job eliminado")


@app.get("/admin/users")
async def admin_users(request: Request, user=Depends(auth.require_admin)):
    rows = utils.list_users()
    message = request.query_params.get("msg")
    return _render(request, "admin_users.html", _context(request, {"users": rows, "message": message, "user": user}))


@app.post("/admin/users/{username}/role")
async def admin_user_role(
    username: str,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    role: str = Form(...),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    role_norm = role.strip().lower()
    if role_norm not in ALLOWED_ROLES:
        return _redirect("/admin/users", "Rol no permitido.")
    row = utils.get_user_by_username(username)
    if not row:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    utils.set_user_role(username, role_norm)
    return _redirect("/admin/users", f"Rol de {username} actualizado.")


@app.post("/admin/users/{username}/activate")
async def admin_user_activate(
    username: str,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    row = utils.get_user_by_username(username)
    if not row:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    utils.set_user_active(username, True)
    return _redirect("/admin/users", f"Usuario {username} activado.")


@app.post("/admin/users/{username}/deactivate")
async def admin_user_deactivate(
    username: str,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    row = utils.get_user_by_username(username)
    if not row:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    utils.set_user_active(username, False)
    return _redirect("/admin/users", f"Usuario {username} desactivado.")


@app.get("/admin/tenants")
async def admin_tenants(request: Request, user=Depends(auth.require_admin)):
    tenants = config.list_tenants(include_defaults=True)
    raw = config.list_tenants(include_defaults=False)
    message = request.query_params.get("msg")
    return _render(
        request,
        "admin_tenants.html",
        _context(
            request,
            {
                "tenants": tenants,
                "raw_tenants": raw,
                "message": message,
                "user": user,
            },
        ),
    )


@app.post("/admin/tenants/create")
async def admin_tenant_create(
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    name: str = Form(...),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant_name = name.strip()
    if not tenant_name:
        return _redirect("/admin/tenants", "Nombre de tenant inválido")
    raw = config.list_tenants(include_defaults=False)
    if tenant_name in raw:
        return _redirect(f"/admin/tenants/{tenant_name}", "El tenant ya existe")
    config.save_tenant_config(tenant_name, {})
    config.reload_tenants_config()
    return _redirect(f"/admin/tenants/{tenant_name}", "Tenant creado, completa la configuración")


@app.get("/admin/tenants/{tenant}")
async def admin_tenant_detail(tenant: str, request: Request, user=Depends(auth.require_admin)):
    raw = config.list_tenants(include_defaults=False).get(tenant, {})
    enriched = config.get_tenant_config(tenant)
    message = request.query_params.get("msg")
    return _render(
        request,
        "admin_tenant_edit.html",
        _context(
            request,
            {
                "tenant_name": tenant,
                "config": enriched,
                "raw_config": raw,
                "message": message,
                "user": user,
            },
        ),
    )


@app.post("/admin/tenants/{tenant}")
async def admin_tenant_save(
    tenant: str,
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    erp: str = Form("a3innuva"),
    default_journal: str = Form("COMPRAS"),
    supplier_account: str = Form("410000"),
    sales_journal: str = Form("VENTAS"),
    customer_account: str = Form("430000"),
    notes: str = Form(""),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    payload = {
        "erp": erp.strip() or "a3innuva",
        "default_journal": default_journal.strip() or "COMPRAS",
        "supplier_account": supplier_account.strip() or "410000",
        "sales_journal": sales_journal.strip() or "VENTAS",
        "customer_account": customer_account.strip() or "430000",
    }
    if notes.strip():
        payload["notes"] = notes.strip()
    config.save_tenant_config(tenant, payload)
    config.reload_tenants_config()
    return _redirect(f"/admin/tenants/{tenant}", "Tenant actualizado")


@app.get("/admin/fiscal")
async def admin_fiscal(request: Request, user=Depends(auth.require_admin)):
    today = date.today()
    first_day = today.replace(day=1)
    message = request.query_params.get("msg")
    return _render(
        request,
        "admin_fiscal.html",
        _context(
            request,
            {
                "default_tenant": settings.default_tenant,
                "period": {"from": first_day.isoformat(), "to": today.isoformat()},
                "message": message,
                "user": user,
            },
        ),
    )


@app.post("/admin/fiscal/sii")
async def admin_fiscal_sii(
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    tenant: Optional[str] = Form(None),
    date_from: str = Form(...),
    date_to: str = Form(...),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    path = sii_export.write_sii_file(tenant or None, date_from, date_to)
    return _redirect("/admin/fiscal", f"SII exportado en {path.name}")


@app.post("/admin/fiscal/facturae")
async def admin_fiscal_facturae(
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    doc_id: str = Form(...),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    path = facturae_export.write_facturae_file(doc_id.strip())
    return _redirect("/admin/fiscal", f"Facturae generado en {path.name}")


@app.post("/admin/fiscal/verifactu")
async def admin_fiscal_verifactu(
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    doc_id: str = Form(...),
    action: str = Form("ALTA"),
    user=Depends(auth.require_admin),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    payload = efactura_payloads.build_verifactu_record(doc_id.strip(), action)
    path = efactura_payloads.write_payload(payload, f"verifactu_{doc_id}_{action}.json")
    return _redirect("/admin/fiscal", f"VeriFactu guardado en {path.name}")


@app.get("/assistant")
async def assistant_form(request: Request, user=Depends(auth.require_user)):
    tenant = auth.current_tenant(request, user)
    return _render(
        request,
        "assistant.html",
        _context(
            request,
            {
                "question": "",
                "doc_id": "",
                "doc_action": "issues",
                "response": "",
                "user": user,
                "active_tenant": tenant,
            },
        ),
    )


@app.post("/assistant")
async def assistant_post(
    request: Request,
    csrf_token: str = Form(..., alias="_csrf_token"),
    question: str = Form(""),
    doc_id: str = Form(""),
    doc_action: str = Form("issues"),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    question = (question or "").strip()
    doc_id = (doc_id or "").strip()
    tenant = auth.current_tenant(request, user)
    response = "Debes introducir una pregunta o un doc_id."
    if doc_id:
        doc_row = utils.get_doc(doc_id)
        if not doc_row or doc_row["tenant"] != tenant:
            raise HTTPException(status_code=404, detail="Documento no encontrado")
        if doc_action == "entry":
            response = rag_normativo.explain_entry_choice(doc_id, tenant=tenant, user=user["username"])
        else:
            response = rag_normativo.explain_doc_issues(doc_id, tenant=tenant, user=user["username"])
    elif question:
        response = rag_normativo.answer_normative_question(question, tenant=tenant, user=user["username"])
    return _render(
        request,
        "assistant.html",
        _context(
            request,
            {
                "question": question,
                "doc_id": doc_id,
                "doc_action": doc_action,
                "response": response,
                "user": user,
                "active_tenant": tenant,
            },
        ),
    )


@app.get("/demo-upload")
async def demo_upload_form(request: Request, user=Depends(auth.require_user)):
    tenant = auth.current_tenant(request, user)
    return _render(
        request,
        "demo_upload.html",
        _context(
            request,
            {"user": user, "active_tenant": tenant, "result": None},
        ),
    )


@app.post("/demo-upload")
async def demo_upload(
    request: Request,
    background_tasks: BackgroundTasks,
    csrf_token: str = Form(..., alias="_csrf_token"),
    tenant_override: Optional[str] = Form(None),
    upload_file: UploadFile = File(...),
    user=Depends(auth.require_user),
):
    auth.verify_origin(request)
    auth.verify_csrf(request, csrf_token)
    tenant = auth.current_tenant(request, user)
    if user.get("role") == "admin" and tenant_override:
        tenant = tenant_override
    upload_dir = utils.BASE_DIR / "IN" / tenant / "web_uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    safe_name = Path(upload_file.filename or "document").name
    target_path = upload_dir / safe_name
    with target_path.open("wb") as out:
        out.write(upload_file.file.read())

    def _run_pipeline(path: Path, tenant: str) -> None:
        utils.configure_logging()
        try:
            pipeline.process_file(path, tenant=tenant, force=True)
        except Exception:
            logger.exception("Procesamiento async falló para %s", path)

    background_tasks.add_task(_run_pipeline, target_path, tenant)
    result = {
        "doc_id": utils.compute_sha256(target_path),
        "status": "RECEIVED",
        "tenant": tenant,
        "note": "Procesamiento en background iniciado. Revisa el dashboard/cola HITL para ver el estado.",
    }
    return _render(
        request,
        "demo_upload.html",
        _context(
            request,
            {"user": user, "active_tenant": tenant, "result": result},
        ),
    )


@app.get("/reports/explain")
async def explain_report_view(
    request: Request,
    report_type: str,
    tenant: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    months: int = 3,
    user=Depends(auth.require_user),
):
    current_tenant = auth.current_tenant(request, user)
    if user.get("role") != "admin":
        tenant = current_tenant
    else:
        tenant = tenant or current_tenant
    today = date.today()
    if not date_from:
        date_from = today.replace(day=1).isoformat()
    if report_type != "cashflow" and not date_to:
        date_to = today.isoformat()

    if report_type == "pnl":
        report = reports_module.build_pnl(tenant, date_from, date_to)
        explanation = explain_reports.explain_pnl(report, tenant=tenant, user=user["username"])
    elif report_type == "iva":
        report = reports_module.build_vat_report(tenant, date_from, date_to)
        explanation = explain_reports.explain_vat(report, tenant=tenant, user=user["username"])
    elif report_type == "cashflow":
        report = reports_module.build_cashflow_forecast(tenant, date_from, months)
        explanation = explain_reports.explain_cashflow(report, tenant=tenant, user=user["username"])
    else:
        raise HTTPException(status_code=400, detail="Tipo de informe no soportado")
    return _render(
        request,
        "report_explanation.html",
        _context(
            request,
            {
                "report_type": report_type,
                "tenant": tenant,
                "date_from": date_from,
                "date_to": date_to,
                "months": months,
                "explanation": explanation,
                "user": user,
                "active_tenant": tenant,
            },
        ),
    )


@app.get("/metrics")
async def metrics_endpoint():
    payload = _prometheus_snapshot()
    return Response(content=payload, media_type="text/plain; version=0.0.4")


@app.get("/healthz")
async def healthz():
    try:
        with utils.get_connection() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"db_unhealthy: {exc}")


@app.get("/readyz")
async def readyz():
    try:
        details = health.check_readiness()
        return {"status": "ready", "details": details}
    except health.ReadinessError as exc:
        raise HTTPException(status_code=503, detail=exc.details)
