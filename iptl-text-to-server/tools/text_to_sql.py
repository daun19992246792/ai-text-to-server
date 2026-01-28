from typing import Optional, Dict, Any, List
import os
from tools.text2sql.config import Text2SQLConfig
from tools.text2sql.engine import Text2SQLEngine
from tools.text2sql.factory import build_text2sql_config_from_global
from tools.sql_vaildator import SQLSecurityChecker
from tools.sql_executor import SQLExecutor
from resources.config.config_save import get_global_db_config, get_global_model_config

class Text2SQLService:
    """
    Service to handle Text2SQL operations with cached components.
    """
    def __init__(self):
        self._engine: Optional[Text2SQLEngine] = None
        self._executor: Optional[SQLExecutor] = None
        self._checker: Optional[SQLSecurityChecker] = None

    def _initialize(self):
        if self._engine is not None:
            return

        config = build_text2sql_config_from_global()
        
        self._engine = Text2SQLEngine(config)

        self._executor = SQLExecutor(self._engine.db_manager.engine)

        self._checker = SQLSecurityChecker(max_limit=50)

    def query(self, query: str) -> list[dict[str, str | Any]] | dict[str, str]:
        """
        Text -> SQL -> Validate -> Execute
        """
        try:
            self._initialize()

            generated_sql = self._engine.query(query)
            dialect = self._engine.db_manager.sql_database.dialect

            mapping = {
                "postgresql": "postgres",
                "mysql": "mysql",
                "sqlite": "sqlite",
                "oracle": "oracle",
            }
            dialect = mapping.get(dialect.lower(), dialect)

            print(f"LLM生成的SQL: {generated_sql}")
            validated_sql = self._checker.validata(generated_sql, dialect=dialect)

            print(f"验证后的SQL: {validated_sql}")
            results = self._executor.execute(validated_sql)

            # 截断结果中的过长字符串, 并且截断行数
            truncated_results = []
            for row in results[:100]:
                new_row = {}
                for k, v in row.items():
                    if isinstance(v, (str, int, float, bool, type(None))):
                        val = v
                    else:
                        val = str(v)

                    if isinstance(v, str) and len(v) > 100:
                        new_row[k] = val[:100] + "..."
                    else:
                        new_row[k] = val
                truncated_results.append(new_row)

            return truncated_results

        except Exception as e:
            return {
                "error": str(e),
                "query": query
            }

_SERVICE = Text2SQLService()

def query_database(query: str) -> Dict[str, Any]:
    """
    使用自然语言查询数据库。
    输入: 用户的自然语言问题 (例如: "查询最近的5个订单")
    输出: SQL查询结果
    """
    return _SERVICE.query(query)
