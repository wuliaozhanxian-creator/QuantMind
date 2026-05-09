import json
import logging
import os
import re

from openai import AsyncOpenAI

from .prompts import SQL_GENERATOR_SYSTEM_PROMPT_DYNAMIC
from .schema_retriever import TABLE_DESCRIPTIONS, get_schema_retriever

logger = logging.getLogger(__name__)


class SQLGenerator:
    def __init__(self):
        api_key = os.getenv("QWEN_API_KEY") or os.getenv("DASHSCOPE_API_KEY")
        base_url = os.getenv("DASHSCOPE_BASE_URL") or "https://dashscope.aliyuncs.com/compatible-mode/v1"

        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = os.getenv("QWEN_MODEL", "qwen-max")

    async def generate_sql(self, parsed_intent: dict) -> str:
        try:
            target_table = parsed_intent.get("target_table") or "stock_selection"
            retriever = await get_schema_retriever()
            schema_info = await retriever.retrieve(parsed_intent.get("query", ""), top_k=12)
            allowed_fields = parsed_intent.get("allowed_fields") or schema_info.get("allowed_fields") or []

            required_select = self._build_required_select(target_table)
            prompt = SQL_GENERATOR_SYSTEM_PROMPT_DYNAMIC.format(
                target_table=target_table,
                table_description=TABLE_DESCRIPTIONS.get(target_table, ""),
                allowed_fields=", ".join(allowed_fields),
                required_select=required_select,
                intent_json=json.dumps(parsed_intent, ensure_ascii=False),
            )

            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": "请生成SQL"},
                ],
                temperature=0.0,
            )
            sql = response.choices[0].message.content.strip()
            if sql.startswith("```sql"):
                sql = sql.replace("```sql", "").replace("```", "").strip()
            elif sql.startswith("```"):
                sql = sql.replace("```", "").strip()

            # 修复 LLM 可能生成的错误表名
            sql = sql.replace("stock_daily_latest_latest", "stock_daily_latest")
            sql = sql.replace("stock_selection_selection", "stock_selection")

            if not self._validate_sql(sql, target_table, allowed_fields):
                raise ValueError("SQL 校验失败：包含非法语句或字段")
            return sql
        except Exception as e:
            logger.error(f"Selection SQL generation failed: {e}")
            return ""

    @staticmethod
    def _build_required_select(table: str) -> str:
        # 简化 SELECT 字段，只返回核心字段
        return (
            "SELECT symbol, name, close, total_mv, pe_ttm, pb, roe\n"
            f"FROM {table}\n"
            "WHERE ..."
        )

    @staticmethod
    def _validate_sql(sql: str, target_table: str, allowed_fields: list[str]) -> bool:
        sql_lower = sql.lower()
        if not sql_lower.startswith("select"):
            return False
        if ";" in sql_lower:
            return False
        forbidden = [
            "insert",
            "update",
            "delete",
            "drop",
            "alter",
            "create",
            "truncate",
        ]
        if any(k in sql_lower for k in forbidden):
            return False
        if f"from {target_table}" not in sql_lower:
            return False

        tokens = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", sql_lower)
        keywords = {
            "select",
            "from",
            "where",
            "and",
            "or",
            "not",
            "in",
            "is",
            "null",
            "like",
            "between",
            "exists",
            "order",
            "by",
            "limit",
            "offset",
            "asc",
            "desc",
            "join",
            "left",
            "right",
            "inner",
            "outer",
            "on",
            "group",
            "having",
            "as",
            "case",
            "when",
            "then",
            "else",
            "end",
            "distinct",
            "cast",
            "coalesce",
            "max",
            "min",
            "sum",
            "avg",
            "count",
        }
        allowed = set(allowed_fields) | {target_table}
        for t in tokens:
            if t in keywords:
                continue
            if t.startswith("t") and t[1:].isdigit():
                continue
            if t in allowed:
                continue
            # 容许别名 symbol/name
            if t in {"symbol", "name"}:
                continue
            return False
        return True


_generator = None


def get_sql_generator():
    global _generator
    if _generator is None:
        _generator = SQLGenerator()
    return _generator
