import logging
import os

from resources.config.config_save import get_global_db_config, get_global_model_config
from .config import Text2SQLConfig
from .engine_manager import EnginePool

logger = logging.getLogger(__name__)

def build_text2sql_config_from_global() -> Text2SQLConfig:
    """
    从全局配置(resources.config)或环境变量构建 Text2SQLConfig 对象。
    如果数据库和模型未配置且无环境变量，抛出 ValueError。
    """
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
        # Fallback to environment variable
        db_uri = os.getenv("DB_URI", "")

    if not db_uri:
        raise ValueError("数据库尚未配置")

    config = Text2SQLConfig(db_uri=db_uri)
    
    model_config = get_global_model_config()
    if model_config:
         if model_config.basic_model.model != 'string':
            config.llm_model_name = model_config.basic_model.model
            config.llm_api_key = model_config.basic_model.api_key
            config.llm_api_base = model_config.basic_model.base_url
         else:
             raise ValueError("LLM 尚未配置")

         if model_config.embedding_model.model != 'string':
            config.embedding_model_name = model_config.embedding_model.model
            config.embedding_api_key = model_config.embedding_model.api_key
            config.embedding_api_base = model_config.embedding_model.base_url
         else:
             raise ValueError("嵌入模型尚未配置")
    else:
        raise ValueError("模型尚未配置")

    return config

def warm_up_engine_task():
    """
    用于后台任务的引擎预热函数。
    捕获异常并记录日志，避免崩溃。
    """
    try:
        logger.info("[Factory] Starting async engine warm-up...")
        config = build_text2sql_config_from_global()
        EnginePool().warm_up(config)
        logger.info("[Factory] Engine warm-up completed.")
    except Exception as e:
        logger.error(f"[Factory] Engine warm-up failed: {e}", exc_info=True)
