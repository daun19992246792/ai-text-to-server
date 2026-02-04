import logging
from threading import Lock
from cachetools import TTLCache
from .config import Text2SQLConfig
from .engine import Text2SQLEngine

logger = logging.getLogger(__name__)

class EngineCache(TTLCache):
    """
    继承 TTLCache 以实现资源回收回调。
    当缓存项因过期(TTL)或超出大小(maxsize)被移除时，popitem 会被调用。
    """
    def popitem(self):
        key, value = super().popitem()
        self._dispose_resource(key, value)
        return key, value

    def _dispose_resource(self, key, engine: Text2SQLEngine):
        try:
            logger.info(f"[EnginePool] Evicting engine for config_id: {key}. Disposing resources...")
            if hasattr(engine, 'db_manager') and engine.db_manager.engine:
                engine.db_manager.engine.dispose()
                logger.info(f"[EnginePool] Database connection disposed for config_id: {key}")
        except Exception as e:
            logger.error(f"[EnginePool] Error disposing engine for {key}: {e}")

class EnginePool:
    """
    单例引擎池，管理 Text2SQLEngine 实例。
    """
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(EnginePool, cls).__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        # ttl=1800s (30分钟), maxsize可根据服务器内存调整
        self.cache = EngineCache(maxsize=10, ttl=1800)
        self.pool_lock = Lock()

    def get_engine(self, config: Text2SQLConfig) -> Text2SQLEngine:
        """
        获取或创建引擎实例。如果是新创建，会触发初始化流程（含DDL扫描和向量索引加载）。
        """
        key = config.config_id
        
        with self.pool_lock:
            if key in self.cache:
                logger.info(f"[EnginePool] Cache hit for config_id: {key}")
                return self.cache[key]
            
            # 双重检查
            if key in self.cache:
                return self.cache[key]

            logger.info(f"[EnginePool] Cache miss. Initializing new engine for config_id: {key}")
            try:
                # 实例化引擎（耗时操作）
                # 注意：这里仍在锁内，防止并发创建同一个config的多个实例
                # 对于不同的config，理想情况下应该允许并发，但在Python GIL下
                # 以及为了简化实现，这里使用全局锁也是可接受的，
                # 除非初始化非常非常慢且高并发。
                # 考虑到初始化涉及大量IO（DB, 向量库），如果需要更高并发，
                # 可以使用基于key的锁（如 weakref Dict[str, Lock]）。
                engine = Text2SQLEngine(config)
                self.cache[key] = engine
                return engine
            except Exception as e:
                logger.error(f"[EnginePool] Failed to initialize engine: {e}")
                raise e

    def warm_up(self, config: Text2SQLConfig):
        """
        预热引擎（通常用于后台任务）
        """
        logger.info(f"[EnginePool] Warming up engine for config_id: {config.config_id}")
        self.get_engine(config)
