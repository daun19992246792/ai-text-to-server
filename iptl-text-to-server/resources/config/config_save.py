from typing import Dict, Any, Tuple
import threading
from fastapi import HTTPException

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from resources.config.config import DatabaseType, DatabaseConfig, ModelConfig, PromptsConfig

# 线程锁：保证配置更新的原子性
_config_lock = threading.Lock()

# 私有变量：存储数据库配置（仅内部可修改）
_db_config: Dict[str, Any] = {}
_model_config: Dict[str, Any] = {}
_prompts_config: Dict[str, Any] = {}

def update_global_db_config(config: Dict[str, Any]) -> None:
    """
    更新全局数据库配置（唯一可修改入口）
    :param config: 数据库连接参数字典
    """
    global _db_config

    if _db_config:
        raise HTTPException(status_code=400, detail=f"已配置数据库，不支持修改")

    # 加锁保证线程安全
    with _config_lock:
        # 更新私有变量
        _db_config = config.copy()


def get_global_db_config():
    """
    获取只读的全局数据库配置（外部唯一读取入口）
    :return: 只读配置实例，未配置则返回None
    """
    if not _db_config:
        return None
    return DatabaseConfig(**_db_config)


def validate_db_connection(db_config: Dict[str, Any]) -> Tuple[bool, str]:
    """
    验证数据库连接信息是否有效
    :param db_config: 数据库连接参数字典
    :return: (是否成功, 消息)
    """
    try:
        # 1. 提取核心参数并校验
        host = db_config.get("host")
        port = db_config.get("port")
        username = db_config.get("username")
        password = db_config.get("password")
        database_name = db_config.get("database_name")
        database_type = db_config.get("database_type", DatabaseType.PostgreSQL)

        # 2. 构建数据库连接URL（支持MySQL/PostgreSQL/Oracle/SQL Server）
        if isinstance(database_type, DatabaseType):
            # 连接URL格式：mysql+pymysql://user:password@host:port/dbname
            connection_url = (
                f"{database_type.get_driver()}://{username}:{password}@{host}:{port}/{database_name}"
            )
        # elif database_type == DatabaseType.SQLite:
        #     # SQLite连接URL格式：sqlite:///path/to/db/file.db
        #     connection_url = f"sqlite:///{database_name}"
        else:
            return False, f"不支持的数据库类型：{database_type.value}（仅支持mysql/postgresql/oracle/SQL server）"

        # 3. 创建引擎并测试连接（设置短超时，避免长时间阻塞）
        engine = create_engine(
            connection_url,
            connect_args={"connect_timeout": 5},  # 连接超时5秒
            pool_pre_ping=True  # 检查连接是否有效
        )

        # 4. 尝试建立连接
        with engine.connect() as conn:
            # 执行简单查询验证连接（不同数据库通用）
            normal_sql = "SELECT 1"
            if database_type == DatabaseType.Oracle:
                normal_sql="SELECT 1 FROM DUAL"
            result = conn.execute(text(normal_sql))
            if result.scalar() == 1:
                return True, "数据库连接验证成功"

    except SQLAlchemyError as e:
        # 捕获SQLAlchemy相关异常，返回具体错误信息
        error_msg = str(e).split("\n")[0]  # 简化错误信息
        return False, f"数据库连接失败：{error_msg}"
    except Exception as e:
        # 捕获其他异常（如参数错误、网络错误等）
        return False, f"验证失败：{str(e)}"

    return True, "数据库连接验证成功"

def update_global_model_config(config: Dict[str, Any]) -> None:
    """
    更新全局模型配置（唯一可修改入口）
    :param config: 模型连接参数字典
    """
    global _model_config

    if _model_config:
        raise HTTPException(status_code=400, detail=f"已配置模型，不支持修改")

    # 加锁保证线程安全
    with _config_lock:

        # 更新私有变量
        _model_config = config.copy()


def get_global_model_config():
    """
    获取只读的全局模型配置（外部唯一读取入口）
    :return: 只读配置实例，未配置则返回None
    """
    if not _model_config:
        return None
    return ModelConfig(**_model_config)

def validate_model(model_config: Dict[str, Any]) -> Tuple[bool, str]:
    if not model_config or not model_config.get("basic_model") or not model_config.get("embedding_model"):
        return False, "模型不能为空"
    return True, "模型配置正常"

def update_global_prompts_config(config: Dict[str, Any]) -> None:
    """
    更新全局模型提示词配置（唯一可修改入口）
    :param config: 模型提示词参数字典
    """
    global _prompts_config

    # 加锁保证线程安全
    with _config_lock:
        # todo 验证必要参数

        # 更新私有变量
        _prompts_config = config.copy()


def get_global_prompts_config() -> PromptsConfig | None:
    """
    获取全局模型提示词配置（外部唯一读取入口）
    :return: 模型提示词参数字典，未配置则返回None
    """
    if not _prompts_config:
        return None
    # 返回不可修改的实例
    return PromptsConfig(**_prompts_config)