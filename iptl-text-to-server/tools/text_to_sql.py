from typing import Optional, Dict, Any, List
import os
from tools.text2sql.config import Text2SQLConfig
from tools.text2sql.engine import Text2SQLEngine
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

        db_config = get_global_db_config()
        db_uri = ""
        
        if db_config:
            db_type = str(db_config.database_type)
            driver_map = {
                "PostgreSQL": "postgresql+psycopg2",
                "MySQL": "mysql+pymysql",
                "Oracle": "oracle+cx_oracle",
                "SQLServer ": "mssql+pyodbc",
                "SQLite": "sqlite"
            }
            scheme = driver_map.get(db_type, "postgresql+psycopg2")
            
            if scheme == "sqlite":
                 db_uri = f"sqlite:///{db_config.database_name}"
            else:
                 db_uri = f"{scheme}://{db_config.username}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.database_name}"
        else:
             db_uri = os.getenv("DB_URI", "")
        
        if not db_uri:
             raise ValueError("Database configuration is missing. Please configure the database connection.")

        config = Text2SQLConfig(db_uri=db_uri)
        
        model_config = get_global_model_config()
        if model_config:
             if model_config.basic_model:
                config.llm_model_name = model_config.basic_model.model
                config.llm_api_key = model_config.basic_model.api_key
                config.llm_api_base = model_config.basic_model.base_url
             
             if model_config.embedding_model:
                config.embedding_model_name = model_config.embedding_model.model
                config.embedding_api_key = model_config.embedding_model.api_key
                config.embedding_api_base = model_config.embedding_model.base_url
        
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
            print(f'LLM 生成的SQL: {generated_sql}')
            dialect = self._engine.db_manager.sql_database.dialect

            mapping = {
                "postgresql": "postgres",
                "mysql": "mysql",
                "sqlite": "sqlite",
                "oracle": "oracle",
            }
            dialect = mapping.get(dialect.lower(), dialect)

            validated_sql = self._checker.validata(generated_sql, dialect=dialect)
            print(f'校验后的SQL: {validated_sql}')

            results = self._executor.execute(validated_sql)

            # 截断结果中的过长字符串
            truncated_results = []
            for row in results:
                new_row = {}
                for k, v in row.items():
                    if isinstance(v, str) and len(v) > 100:
                        new_row[k] = v[:100] + "..."
                    else:
                        new_row[k] = v
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
