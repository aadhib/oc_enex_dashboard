from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Literal, TypedDict

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .app_db import get_user_by_id
from .config import settings

TOKEN_COOKIE_NAME = "oc_hr_admin_token"
security = HTTPBearer(auto_error=False)


class AuthUser(TypedDict):
    id: int
    email: str
    username: str
    role: Literal["admin", "inspector"]


def create_access_token(user: AuthUser) -> tuple[str, int]:
    issued_at = datetime.now(timezone.utc)
    expires_at = issued_at + timedelta(minutes=settings.jwt_expires_minutes)
    payload = {
        "sub": str(user["id"]),
        "uid": user["id"],
        "username": user["username"],
        "email": user["email"],
        "role": user["role"],
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    encoded = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return encoded, settings.jwt_expires_minutes * 60


def decode_access_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except jwt.PyJWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        ) from exc


def _resolve_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None,
) -> str:
    if credentials:
        if credentials.scheme.lower() != "bearer":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication scheme",
            )
        return credentials.credentials

    cookie_token = request.cookies.get(TOKEN_COOKIE_NAME)
    if cookie_token:
        return cookie_token

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
    )


def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> AuthUser:
    token = _resolve_token(request, credentials)
    payload = decode_access_token(token)

    raw_uid = payload.get("uid") or payload.get("sub")
    if raw_uid is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token subject missing",
        )

    try:
        user_id = int(str(raw_uid))
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token subject",
        ) from exc

    user = get_user_by_id(user_id)
    if not user or not bool(user.get("is_active")):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
        )

    role = str(user.get("role") or "")
    if role == "hr":
        role = "inspector"
    if role not in {"admin", "inspector"}:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user role",
        )

    return {
        "id": int(user["id"]),
        "email": str(user.get("email") or ""),
        "username": str(user.get("username") or ""),
        "role": role,
    }


def require_admin(user: AuthUser = Depends(get_current_user)) -> AuthUser:
    if user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return user


def require_inspector_or_admin(user: AuthUser = Depends(get_current_user)) -> AuthUser:
    if user["role"] not in {"inspector", "admin"}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inspector/Admin access required",
        )
    return user


def require_hr_or_admin(user: AuthUser = Depends(get_current_user)) -> AuthUser:
    # Backward-compatible alias for legacy imports.
    return require_inspector_or_admin(user)
