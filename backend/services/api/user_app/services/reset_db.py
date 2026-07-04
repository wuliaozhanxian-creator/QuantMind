import asyncio
import re
import urllib.parse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from backend.services.api.user_app.config import settings

# SQL 注入防护：合法表名正则（仅允许字母数字下划线）
_VALID_TABLE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


async def reset_db():
    print("WARNING: This will drop all USER SERVICE tables. Other tables will remain.")
    user = urllib.parse.quote_plus(settings.DB_USER)
    password = urllib.parse.quote_plus(settings.DB_PASSWORD)
    db_url = f"postgresql+psycopg2://{user}:{password}@{settings.DB_MASTER_HOST}:{settings.DB_MASTER_PORT}/{settings.DB_NAME}"

    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        # Drop tables in dependency order
        tables_to_drop = [
            "password_reset_tokens",
            "email_verifications",
            "user_audit_logs",
            "role_permissions",
            "user_roles",
            "permissions",
            "roles",
            "login_devices",
            "user_sessions",
            "user_profiles",
            "users",
        ]

        for table in tables_to_drop:
            # SQL 注入防护：校验表名格式（虽然来自硬编码列表，仍做深度防御）
            if not _VALID_TABLE_RE.match(table):
                raise ValueError(f"非法表名（含注入风险）: {table!r}")
            print(f"Dropping {table}...")
            await conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))

        # Reset alembic version
        print("Resetting alembic_version...")
        # Check if table exists first
        try:
            await conn.execute(text("DELETE FROM alembic_version"))
        except Exception as e:
            print(f"Error clearing alembic_version (might not exist): {e}")

    await engine.dispose()
    print("User Service tables reset complete.")


if __name__ == "__main__":
    asyncio.run(reset_db())
