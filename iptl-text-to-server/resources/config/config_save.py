from typing import Dict, Any, Final
import threading
from dataclasses import dataclass

from resources.config.config import DatabaseType, Model

# 线程锁：保证配置更新的原子性
_config_lock = threading.Lock()

# 私有变量：存储数据库配置（仅内部可修改）
_db_config: Dict[str, Any] = {}


# 封装为只读数据类（强化不可修改特性）
@dataclass(frozen=True)  # frozen=True 使实例属性不可修改
class ReadOnlyDBConfig:
    host: str
    port: int
    username: str
    password: str
    database_name: str
    database_type: DatabaseType

    # 转为字典（方便外部使用）
    def to_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": "******" if self.password else "",
            "database_name": self.database_name,
            "database_type": self.type
        }

@dataclass(frozen=True)  # frozen=True 使实例属性不可修改
class ReadOnlyModelConfig:
    basic_model: Model
    embedding_model: Model

    # 转为字典（方便外部使用）
    def to_dict(self) -> Dict[str, Any]:
        return {
            "basic_model": {
                "model": self.basic_model.model,
                "base_url": self.basic_model.base_url,
                "api_key": self.basic_model.api_key,
            },
            "embedding_model": {
                "model": self.embedding_model.model,
                "base_url": self.embedding_model.base_url,
                "api_key": self.embedding_model.api_key,
            }
        }


# 对外暴露的只读配置（Final保证变量本身不可重新赋值）
GLOBAL_DB_CONFIG: Final[ReadOnlyDBConfig | None] = None
GLOBAL_MODEL_CONFIG: Final[ReadOnlyModelConfig | None] = None


def update_global_db_config(config: Dict[str, Any]) -> None:
    """
    更新全局数据库配置（唯一可修改入口）
    :param config: 数据库连接参数字典
    """
    global _db_config

    # 加锁保证线程安全
    with _config_lock:
        # todo 验证数据库连接

        # 更新私有变量
        _db_config = config.copy()


def get_global_db_config() -> ReadOnlyDBConfig | None:
    """
    获取只读的全局数据库配置（外部唯一读取入口）
    :return: 只读配置实例，未配置则返回None
    """
    with _config_lock:
        if not _db_config:
            return None
        # 返回不可修改的实例
        return ReadOnlyDBConfig(**_db_config)

def update_global_model_config(config: Dict[str, Any]) -> None:
    """
    更新全局数据库配置（唯一可修改入口）
    :param config: 数据库连接参数字典
    """
    global _model_config

    # 加锁保证线程安全
    with _config_lock:
        # todo 验证必要参数

        # 设置默认值
        config.setdefault("basic_model", "")
        config.setdefault("embedding_model", "")

        # 更新私有变量
        _model_config = config.copy()


def get_global_dmodel_config() -> ReadOnlyModelConfig | None:
    """
    获取只读的全局数据库配置（外部唯一读取入口）
    :return: 只读配置实例，未配置则返回None
    """
    with _config_lock:
        if not _model_config:
            return None
        # 返回不可修改的实例
        return ReadOnlyModelConfig(**_db_config)