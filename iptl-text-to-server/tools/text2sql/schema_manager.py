from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from llama_index.core.objects import SQLTableSchema
from .db_manager import DatabaseManager

class SchemaManager:
    """生成表的描述"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def get_table_schema(self) -> List[SQLTableSchema]:
        """
        对管理的表创建 SQLTableSchema 对象
        context_str 将包含详细描述, 包括主键/外键信息
        使用多线程加速描述生成
        """
        schemas = []
        table_names = self.db_manager.get_table_names()

        # 获取线程数配置，默认为 5
        max_workers = 5
        if self.db_manager.config:
            max_workers = self.db_manager.config.table_description_threads

        # print(max_workers)
        def _process_table(table_name: str) -> SQLTableSchema:
            """处理单个表的辅助函数"""
            # 获取详细描述(DDL + 键 + 注释)
            description = self.db_manager.get_detailed_table_description(table_name)
            
            # 创建 SQLTableSchema
            return SQLTableSchema(
                table_name=table_name,
                context_str=description
            )

        # 使用线程池并发执行
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_table = {executor.submit(_process_table, table): table for table in table_names}
            
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    schema = future.result()
                    schemas.append(schema)
                except Exception as exc:
                    print(f'Table {table} generated an exception: {exc}')

        return schemas