"""统一认证中间件

安全变更 (T6.2, 2026-07-04):
- 移除 X-Internal-Call header 后门：get_current_user 不再接受 header 绕过 JWT
- 移除硬编码默认密钥 DEFAULT_INTERNAL_CALL_SECRET
- get_internal_call_secret() 改为 fail-fast，未配置时抛 RuntimeError
- 服务间调用将统一迁移至 service JWT（T6.5 完成）

安全变更 (T6.5, 2026-07-04):
- 删除死代码 get_current_user / require_roles（全量 grep 确认无路由依赖，
  路由认证统一由 user_app/middleware/auth.py 与 user_app/services/auth_service.py 承担）
- AuthManager.create_access_token / verify_token 增加 _ensure_secret_configured() fail-fast
- 配合 config/settings.py 移除弱默认值 "dev-secret-key"（改为空字符串）
- 保留 get_internal_call_secret / AuthManager / create_service_token / decode_jwt_token，
  供服务间调用与各服务 JWT 解码继续使用；service JWT 完整迁移方案见 T6.5_service_jwt_flow.md
- 注意：shared/auth.py 暂不删除（指令⑤），待 service JWT 迁移全部完成后另行清理
"""

import os
import warnings
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.context import CryptContext

from config.settings import settings

from .logging_config import get_logger

logger = get_logger(__name__)
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# 已废弃的硬编码默认密钥（仅用于向后兼容检测，不再作为 fallback）
_DEPRECATED_DEFAULT_SECRET = "dev-internal-call-secret"


def get_internal_call_secret() -> str:
    """获取服务间调用共享密钥。

    安全变更：不再返回硬编码默认值。INTERNAL_CALL_SECRET 未配置时抛 RuntimeError。
    该函数仅供专用内部端点校验使用（如 verify_internal_call），不得用于
    认证绕过场景（get_current_user / require_roles 已于 T6.5 删除）。

    Deprecated: 将在 service JWT 迁移完成后被替代（见 T6.5_service_jwt_flow.md）。
    """
    secret = str(os.getenv("INTERNAL_CALL_SECRET", "")).strip()
    if not secret:
        raise RuntimeError(
            "INTERNAL_CALL_SECRET 未配置。请在 .env 或环境变量中设置该值。"
            "该密钥用于服务间专用内部端点校验，将在 T6.5 中迁移至 service JWT。"
        )
    if secret == _DEPRECATED_DEFAULT_SECRET:
        warnings.warn(
            "INTERNAL_CALL_SECRET 仍使用弱默认值 'dev-internal-call-secret'，"
            "请立即更换为强随机值。",
            stacklevel=2,
        )
    return secret


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


def optional_auth(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> dict[str, Any] | None:
    """可选认证依赖注入函数

    Args:
        credentials: HTTP认证凭据（可选）

    Returns:
        用户信息字典或None
    """
    if credentials is None:
        return None

    try:
        token = credentials.credentials
        payload = auth_manager.verify_token(token)
        return payload
    except HTTPException:
        return None


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
        # 与 get_internal_call_secret / create_service_token 风格保持一致。
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
