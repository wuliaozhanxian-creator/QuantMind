"""统一认证中间件

安全变更 (T6.2, 2026-07-04):
- 移除 X-Internal-Call header 后门：get_current_user 不再接受 header 绕过 JWT
- 移除硬编码默认密钥 DEFAULT_INTERNAL_CALL_SECRET
- 服务间调用将统一迁移至 service JWT（T6.5 完成）

安全变更 (T6.5, 2026-07-04):
- 删除死代码 get_current_user / require_roles（全量 grep 确认无路由依赖，
  路由认证统一由 user_app/middleware/auth.py 与 user_app/services/auth_service.py 承担）
- AuthManager.create_access_token / verify_token 增加 _ensure_secret_configured() fail-fast
- 配合 config/settings.py 移除弱默认值 "dev-secret-key"（改为空字符串）

安全变更 (T6.5-P3, 2026-07-05):
- 删除 get_internal_call_secret() / _DEPRECATED_DEFAULT_SECRET（service JWT 迁移完成）
- 删除 optional_auth() 死代码（全量 grep 确认无路由依赖）
- 服务间认证统一入口：verify_service_token() / require_service_token()
- 用户认证统一入口：AuthManager.verify_token()（user_app/middleware/auth.py 调用）
- INTERNAL_CALL_SECRET 环境变量已废弃，仅在 .env.example 中保留 deprecated 标注
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict

import jwt
from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.context import CryptContext

from config.settings import settings

from .logging_config import get_logger

logger = get_logger(__name__)
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class AuthManager:
    """认证管理器"""

    def __init__(self):
        self.secret_key = settings.security.secret_key
        self.algorithm = settings.security.jwt_algorithm
        self.expire_minutes = settings.security.jwt_expire_minutes

    def _ensure_secret_configured(self) -> None:
        """fail-fast：SECRET_KEY 未配置时抛 RuntimeError（T6.5 与 T6.2 风格一致）。

        settings.security.secret_key 默认值已由 ``dev-secret-key`` 改为空字符串，
        此处在使用密钥签发/验证 JWT 前显式校验，避免用空密钥产生无效签名。
        """
        if not self.secret_key:
            raise RuntimeError(
                "SECRET_KEY 未配置。请在 .env 或环境变量中设置该值，"
                "AuthManager 不得使用空密钥签发/验证 JWT。"
            )

    def create_access_token(self, data: dict[str, Any]) -> str:
        """创建访问令牌

        Args:
            data: 要编码的数据

        Returns:
            JWT令牌字符串
        """
        self._ensure_secret_configured()
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(minutes=self.expire_minutes)
        to_encode.update({"exp": expire})

        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)

        logger.info(f"Access token created for user: {data.get('sub', 'unknown')}")
        return encoded_jwt

    def verify_token(self, token: str) -> dict[str, Any]:
        """验证令牌

        Args:
            token: JWT令牌

        Returns:
            解码后的数据

        Raises:
            HTTPException: 令牌无效时抛出
        """
        self._ensure_secret_configured()
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired",
                headers={"WWW-Authenticate": "Bearer"},
            )
        except jwt.JWTError as e:
            logger.warning(f"Invalid token: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def hash_password(self, password: str) -> str:
        """哈希密码

        Args:
            password: 明文密码

        Returns:
            哈希后的密码
        """
        return pwd_context.hash(password)

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """验证密码

        Args:
            plain_password: 明文密码
            hashed_password: 哈希密码

        Returns:
            密码是否匹配
        """
        return pwd_context.verify(plain_password, hashed_password)


# 全局认证管理器实例
auth_manager = AuthManager()


# ---------------------------------------------------------------------------
# 微服务通用 JWT 解码工具（从环境变量读取密钥，供各服务统一使用）
# ---------------------------------------------------------------------------


def create_service_token(service_name: str, expire_seconds: int = 300) -> str:
    """生成短期 service JWT，用于服务间调用认证（T6.2 临时方案）。

    替代已移除的 X-Internal-Call header 后门。每个服务使用相同的 SECRET_KEY
    签发/验证，payload 中包含 ``service`` 声明标识调用方身份。

    Args:
        service_name: 调用方服务名（如 "api" / "engine" / "trade" / "stream"）
        expire_seconds: 有效期（秒），默认 5 分钟

    Returns:
        JWT token 字符串

    Raises:
        RuntimeError: SECRET_KEY 未配置时抛出
    """
    secret_key = os.getenv("SECRET_KEY") or os.getenv("JWT_SECRET_KEY") or os.getenv("JWT_SECRET")
    if not secret_key:
        raise RuntimeError("SECRET_KEY 未配置，无法签发 service token")
    algorithm = os.getenv("ALGORITHM") or os.getenv("JWT_ALGORITHM") or "HS256"
    try:
        from jose import jwt as jose_jwt
    except ImportError:
        raise RuntimeError("python-jose 未安装，请执行: pip install python-jose[cryptography]")
    now = datetime.utcnow()
    payload = {
        "service": service_name,
        "iat": now,
        "exp": now + timedelta(seconds=expire_seconds),
    }
    return jose_jwt.encode(payload, secret_key, algorithm=algorithm)


def decode_jwt_token(token: str) -> dict:
    """解码并验证 JWT Token，返回 payload 字典。

    从环境变量动态读取密钥（支持统一配置注入）：
    - SECRET_KEY / JWT_SECRET_KEY / JWT_SECRET
    - ALGORITHM / JWT_ALGORITHM

    Raises:
        RuntimeError: SECRET_KEY 未配置时抛出（fail-fast，不再回退到 dev-secret）。
        HTTPException 401: token 无效或已过期。
    """
    import os

    try:
        from jose import JWTError
        from jose import jwt as jose_jwt
    except ImportError:  # pragma: no cover
        raise RuntimeError("python-jose 未安装，请执行: pip install python-jose[cryptography]")

    secret_key = os.getenv("SECRET_KEY") or os.getenv("JWT_SECRET_KEY") or os.getenv("JWT_SECRET")
    if not secret_key:
        # 安全变更 (T6.2): 移除 ``or "dev-secret"`` 硬编码回退，未配置时 fail-fast，
        # 与 create_service_token 风格保持一致。
        raise RuntimeError("SECRET_KEY 未配置，无法解码 JWT token")
    algorithm = os.getenv("ALGORITHM") or os.getenv("JWT_ALGORITHM") or "HS256"

    try:
        return jose_jwt.decode(token, secret_key, algorithms=[algorithm])
    except JWTError as exc:
        logger.warning(f"JWT decode failed: {exc}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ---------------------------------------------------------------------------
# Service JWT 验证层（T6.5-P2 新增）
#
# 委托方 M2 第三轮裁决：service JWT 使用专用 ``X-Service-Token`` header，
# 禁止复用 ``Authorization: Bearer``，理由：
#   1. 纵深防御——攻击面物理隔离，service token 泄漏不影响用户 token
#   2. 审计可区分——日志/网关层一眼区分 service 调用 vs 用户调用
#   3. 授权边界清晰——service token 永不授予 admin 全权
#
# 中间件分工：
#   - ``Authorization: Bearer <user_jwt>``  → 用户 JWT，由 user_app/middleware/auth.py 处理
#   - ``X-Service-Token: <service_jwt>``    → service JWT，由 require_service_token 处理
#
# 迁移路径详见 ``T6.5_service_jwt_flow.md`` 第 5 节。
# ---------------------------------------------------------------------------

# 允许签发 service JWT 的服务名白名单（防止伪造未登记服务名）
_VALID_SERVICE_NAMES = frozenset({"api", "engine", "trade", "stream"})


def verify_service_token(token: str, allowed_services: list[str]) -> dict:
    """验证 service JWT 并校验调用方服务是否在允许列表内。

    复用 T6.2 已实现的 ``decode_jwt_token`` 进行验签与过期校验，
    在其上叠加 ``service`` 声明白名单校验层。

    安全约束：
    - service token 永不授予 admin 全权：本函数仅返回 payload，不注入任何
      role/admin 声明；调用方不得据此授予管理员权限。
    - 调用方服务必须显式在 ``allowed_services`` 列表内，否则 403。
    - 即便签名有效，``service`` 声明也必须在 ``_VALID_SERVICE_NAMES`` 集合内，
      防止伪造未登记服务名。

    Args:
        token: service JWT 字符串（来自 ``X-Service-Token`` header）
        allowed_services: 允许调用本端点的服务名列表，如 ``["api", "engine"]``

    Returns:
        解码后的 payload 字典，至少包含 ``service`` / ``iat`` / ``exp``

    Raises:
        HTTPException 401: token 无效或已过期（由 ``decode_jwt_token`` 抛出）；
            或 payload 缺失 ``service`` 声明
        HTTPException 403: service 声明未登记，或不在 ``allowed_services`` 内
    """
    payload = decode_jwt_token(token)  # 复用 T6.2 验签底层（fail-fast + 401）
    service = payload.get("service")
    if not service:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Service token missing 'service' claim",
        )
    if service not in _VALID_SERVICE_NAMES:
        # 防御层：即便签名有效，service 声明也必须在已知服务名集合内
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Service '{service}' is not a registered service",
        )
    if service not in allowed_services:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Service '{service}' not allowed for this endpoint",
        )
    return payload


def require_service_token(*allowed_services: str):
    """FastAPI 依赖：要求请求携带合法 service JWT 且调用方在允许列表内。

    service JWT 通过专用 ``X-Service-Token`` header 传递，**不复用**
    ``Authorization: Bearer``，以与用户 JWT 在中间件层物理隔离
    （委托方 M2 第三轮裁决）。

    用法::

        # 路由级依赖（不取 payload）
        @router.post(
            "/internal/...",
            dependencies=[Depends(require_service_token("api", "engine"))],
        )
        def handler(...): ...

        # 或在签名中取 payload
        def handler(payload: dict = Depends(require_service_token("api"))): ...

    Args:
        *allowed_services: 允许调用本端点的服务名可变参数

    Returns:
        FastAPI 依赖函数 ``checker``，返回 payload 字典
    """
    allowed = list(allowed_services)

    def checker(
        x_service_token: str | None = Header(default=None, alias="X-Service-Token"),
    ) -> dict:
        if not x_service_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing service token (X-Service-Token header required)",
            )
        return verify_service_token(x_service_token, allowed)

    return checker
