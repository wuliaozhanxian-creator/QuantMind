"""
兼容配置模块（供 legacy `from config.settings import settings` 使用）。
"""

import os
from dataclasses import dataclass


@dataclass
class LoggingSettings:
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_format: str = os.getenv(
        "LOG_FORMAT", "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    log_file: str = os.getenv("LOG_FILE", "")
    max_log_size: int = int(os.getenv("MAX_LOG_SIZE", str(10 * 1024 * 1024)))
    backup_count: int = int(os.getenv("LOG_BACKUP_COUNT", "5"))


@dataclass
class SecuritySettings:
    # 安全变更 (T6.5): 移除弱默认值 "dev-secret-key"，改为空字符串。
    # 未配置时由 AuthManager 在签发/验证 JWT 时 fail-fast（抛 RuntimeError），
    # 与 shared/auth.py 的 get_internal_call_secret / decode_jwt_token 风格一致。
    secret_key: str = os.getenv("SECRET_KEY", "")
    jwt_algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")
    jwt_expire_minutes: int = int(os.getenv("JWT_EXPIRE_MINUTES", "2880"))  # 48 hours


@dataclass
class DatabaseSettings:
    postgres_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://quantmind:admin123@192.168.1.88:6789/quantmind",
    )


class Settings:
    def __init__(self):
        self.logging = LoggingSettings()
        self.security = SecuritySettings()
        self.database = DatabaseSettings()
        self.edition = os.getenv("APP_EDITION", "oss")
        self.capabilities = {
            "edition": self.edition,
            "features": {
                "backtest": True,
                "simulation": True,
                "live_trading": False,
                "ai_strategy": True,
                "community": False,
                "subscription": False,
                "sms": False,
            },
        }


settings = Settings()
