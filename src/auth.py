"""Authentication helpers (password hashing, dependencies, CLI user utilities)."""
from __future__ import annotations

import argparse
import getpass
import secrets
from typing import Optional
from urllib.parse import urlparse

from fastapi import Depends, HTTPException, Request, status
from passlib.context import CryptContext

from . import config, utils
from .config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain_password: str) -> str:
    """Return a bcrypt hash of the provided password."""
    if not plain_password:
        raise ValueError("Password vacío")
    return pwd_context.hash(plain_password)


def verify_password(plain_password: str, password_hash: str) -> bool:
    if not plain_password or not password_hash:
        return False
    try:
        return pwd_context.verify(plain_password, password_hash)
    except ValueError:  # pragma: no cover - defensive
        return False


def _row_to_user(row) -> dict:
    return {
        "id": row["id"],
        "username": row["username"],
        "role": row["role"],
        "is_active": bool(row["is_active"]),
    }


def current_user(request: Request) -> Optional[dict]:
    """Return the current logged user as dict or None."""
    username = request.session.get("username")
    if not username:
        return None
    row = utils.get_user_by_username(username)
    if not row or not row["is_active"]:
        return None
    return _row_to_user(row)


def require_user(request: Request) -> dict:
    user = current_user(request)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/login"},
        )
    return user


def require_admin(user: dict = Depends(require_user)) -> dict:
    if user.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Acceso sólo para admin")
    return user


def rotate_session(request: Request, username: Optional[str] = None) -> None:
    """Regenerate the session to mitigate fixation attacks."""
    new_username = username or request.session.get("username")
    request.session.clear()
    if new_username:
        request.session["username"] = new_username
    request.session["_csrf_token"] = secrets.token_hex(32)


def login_user(request: Request, username: str) -> None:
    rotate_session(request, username)


def logout_user(request: Request) -> None:
    request.session.clear()


def verify_origin(request: Request) -> None:
    """Ensure Origin/Referer (when present) matches the configured host."""
    if request.method.upper() not in {"POST", "PUT", "PATCH", "DELETE"}:
        return
    header = request.headers.get("origin") or request.headers.get("referer")
    if not header:
        return
    expected = settings.web_allowed_origin or str(request.base_url)
    expected_parsed = urlparse(expected)
    if not expected_parsed.netloc:
        expected_parsed = urlparse(str(request.base_url))
    received = urlparse(header)
    if not received.scheme or not received.netloc:
        return
    if (received.scheme, received.netloc) != (expected_parsed.scheme, expected_parsed.netloc):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Origen no permitido")


def current_tenant(request: Request, user: dict) -> str:
    """Return the tenant in scope for the current request."""
    tenant = settings.default_tenant
    if user.get("role") == "admin":
        override = request.query_params.get("tenant")
        available = config.list_tenants(include_defaults=True)
        if override and override in available:
            request.session["_tenant_override"] = override
            tenant = override
        else:
            tenant = request.session.get("_tenant_override", tenant)
    return tenant


# --- CSRF utilities ---------------------------------------------------------

def get_csrf_token(request: Request) -> str:
    """Return a CSRF token stored in the session (create if missing)."""
    token = request.session.get("_csrf_token")
    if not token:
        token = secrets.token_hex(32)
        request.session["_csrf_token"] = token
    return token


def verify_csrf(request: Request, token: str) -> None:
    """Ensure the provided token matches the session token."""
    session_token = request.session.get("_csrf_token")
    if not session_token or not token or not secrets.compare_digest(str(session_token), str(token)):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="CSRF token inválido o ausente")


# --- CLI helpers -----------------------------------------------------------------

def _prompt_password() -> str:
    while True:
        pwd = getpass.getpass("Contraseña: ").strip()
        confirm = getpass.getpass("Repite contraseña: ").strip()
        if not pwd:
            print("La contraseña no puede estar vacía.")
            continue
        if pwd != confirm:
            print("Las contraseñas no coinciden, inténtalo de nuevo.")
            continue
        return pwd


def cmd_create_admin(_: argparse.Namespace) -> None:
    username = input("Usuario admin: ").strip()
    if not username:
        raise SystemExit("Usuario obligatorio.")
    existing = utils.get_user_by_username(username)
    if existing:
        raise SystemExit("Ese usuario ya existe.")
    password = _prompt_password()
    password_hash = hash_password(password)
    utils.create_user(username, password_hash, role="admin", is_active=True)
    print(f"Usuario admin '{username}' creado.")


def cmd_list_users(_: argparse.Namespace) -> None:
    rows = utils.list_users()
    if not rows:
        print("No hay usuarios registrados.")
        return
    for row in rows:
        status_flag = "activo" if row["is_active"] else "inactivo"
        print(f"{row['username']} ({row['role']}) - {status_flag}")


def cmd_change_password(args: argparse.Namespace) -> None:
    user = utils.get_user_by_username(args.username)
    if not user:
        raise SystemExit(f"El usuario {args.username} no existe.")
    password = _prompt_password()
    password_hash = hash_password(password)
    utils.update_user_password(args.username, password_hash)
    print(f"Contraseña de '{args.username}' actualizada.")


def _toggle_active(username: str, is_active: bool) -> None:
    user = utils.get_user_by_username(username)
    if not user:
        raise SystemExit(f"El usuario {username} no existe.")
    utils.set_user_active(username, is_active)
    state = "activado" if is_active else "desactivado"
    print(f"Usuario '{username}' {state}.")


def cmd_activate(args: argparse.Namespace) -> None:
    _toggle_active(args.username, True)


def cmd_deactivate(args: argparse.Namespace) -> None:
    _toggle_active(args.username, False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gestión básica de usuarios CERTIVA")
    sub = parser.add_subparsers(dest="command", required=True)

    create_cmd = sub.add_parser("create-admin", help="Crear usuario admin interactivo")
    create_cmd.set_defaults(func=cmd_create_admin)

    list_cmd = sub.add_parser("list", help="Listar usuarios")
    list_cmd.set_defaults(func=cmd_list_users)

    pwd_cmd = sub.add_parser("change-password", help="Cambiar contraseña de un usuario")
    pwd_cmd.add_argument("--username", required=True)
    pwd_cmd.set_defaults(func=cmd_change_password)

    activate_cmd = sub.add_parser("activate", help="Activar usuario")
    activate_cmd.add_argument("--username", required=True)
    activate_cmd.set_defaults(func=cmd_activate)

    deactivate_cmd = sub.add_parser("deactivate", help="Desactivar usuario")
    deactivate_cmd.add_argument("--username", required=True)
    deactivate_cmd.set_defaults(func=cmd_deactivate)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
