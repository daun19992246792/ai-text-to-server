from typing import List, Dict, Any, Union
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

class SQLExecutor:
    """使用 SQLAlchemy 进行 SQL 执行"""
    def __init__(self, engine: Union[Engine, str]):
        if isinstance(engine, str):
            self.engine = create_engine(engine)
        else:
            self.engine = engine

    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """执行 SQL 查询并将结果以字典列表的形式返回"""

        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    stmt = text(sql)
                    result = conn.execute(stmt)

                    # 查询
                    if result.returns_rows:
                        keys = result.keys()
                        rows = [dict(list(zip(keys, row))) for row in result]
                        trans.commit()
                        return rows
                    else:
                        trans.rollback()
                        raise PermissionError('检测到非查询SQL, 操作已回滚并拦截')
                except Exception as e:
                    trans.rollback()
                    raise e
        except Exception as e:
            raise RuntimeError(f'数据库执行错误: {str(e)}')