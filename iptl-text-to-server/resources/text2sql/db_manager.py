from typing import List, Optional, Any
import os
import json
from sqlalchemy import create_engine, inspect, MetaData, Table, select, func
from sqlalchemy.engine import Engine
from llama_index.core import SQLDatabase
from llama_index.llms.openai_like import OpenAILike
from .config import Text2SQLConfig
from .prompts import get_dialect_knowledge

class DatabaseManager:
    """负责管理数据库连接和元数据提取"""

    def __init__(self, db_uri: str, include_tables: Optional[List[str]] = None, config: Text2SQLConfig = None, llm: Optional[OpenAILike] = None):
        self.db_uri = db_uri
        self.include_tables = include_tables
        self.config = config
        self.engine: Engine = create_engine(db_uri)
        self.cache_dir = config.chroma_db_path if config else None
        
        # 优先使用传入的 llm 实例
        if llm:
            self.llm = llm
        # 否则如果配置存在, 则自行创建 
        elif self.config:
            self.llm = OpenAILike(
                model=self.config.llm_model_name,
                api_key=self.config.llm_api_key,
                api_base=self.config.llm_api_base,
                context_window=32768, 
                max_tokens=4096,
                temperature=0.1,
                timeout=300.0,
                reuse_client=False,
                is_chat_model=True,
            )
        else:
            self.llm = None

        # 如果提供了include_tables参数, 则对表进行过滤
        if self.include_tables:
            self._tables = self.include_tables
        else:
            self._tables = self._get_all_table_names()

        self.sql_database = SQLDatabase(self.engine, include_tables=self._tables)

    def _get_cache_path(self) -> Optional[str]:
        if not self.cache_dir:
            return None
        return os.path.join(self.cache_dir, "db_table_names.json")

    def _get_all_table_names(self) -> List[str]:
        cache_path = self._get_cache_path()
        if cache_path and os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load table cache: {e}")

        # 如果没有缓存或加载失败，使用 inspector
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()

        # 写入缓存
        if cache_path:
            try:
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(tables, f, ensure_ascii=False)
            except Exception as e:
                print(f"Warning: Failed to save table cache: {e}")
        
        return tables

    def get_table_names(self) -> List[str]:
        """返回可用表名的列表"""
        return self._tables

    def get_table_info(self, table_name: str) -> str:
        """返回特定表的信息"""
        return self.sql_database.get_single_table_info(table_name)

    def get_detailed_table_description(self, table_name: str) -> str:
        """
        生成包含键信息的详细表描述
        可以扩展基本信息模式, 提供更详细上下文
        当前, 用了 SQLDatabase 的检查功能, 后面可以进行改进
        """

        inspector = inspect(self.engine)

        # 基本列信息
        columns = inspector.get_columns(table_name)
        pk = inspector.get_pk_constraint(table_name)
        fks = inspector.get_foreign_keys(table_name)

        description = f"Table: {table_name}\n"

        # 主键
        if pk and pk.get('constrained_columns'):
            description += f"Primary Key: {', '.join(pk['constrained_columns'])}\n"

        # 外键
        if fks:
            description += "Foreign Keys:\n"
            for fk in fks:
                description += f"  - {', '.join(fk['constrained_columns'])} -> {fk['referred_table']}.{', '.join(fk['referred_columns'])}\n"

        description += "Columns:\n"
        
        # 准备查询样本数据
        metadata = MetaData()
        try:
            table_obj = Table(table_name, metadata, autoload_with=self.engine)
        except Exception as e:
            print(f"Warning: Failed to reflect table {table_name} for sampling: {e}")
            table_obj = None

        with self.engine.connect() as connection:
            for col in columns:
                col_name = col['name']
                col_info = f"  - {col_name} ({col['type']})"
                
                # 获取样本数据
                samples = []
                if table_obj is not None and col_name in table_obj.c:
                    try:
                        # 查询频次最高的5个值
                        stmt = select(table_obj.c[col_name]).group_by(table_obj.c[col_name]).order_by(func.count().desc()).limit(5)
                        result = connection.execute(stmt)
                        samples = [row[0] for row in result]
                    except Exception:
                        pass
                
                if samples:
                    formatted_samples = []
                    for s in samples:
                        s_str = str(s)
                        if len(s_str) > 50:
                            s_str = s_str[:50] + "..."
                        formatted_samples.append(s_str)
                    col_info += f", samples: {formatted_samples}"
                
                # 再添加用户注释
                if col.get('comment'):
                    col_info += f": {col['comment']}"

                description += col_info + "\n"
        
        if self.config and self.config.table_info_for_llm and self.llm:
            description = self.get_detailed_table_description_for_llm(description)

        return description

    def get_detailed_table_description_for_llm(self, description: str) -> str:
        """
        使用 LLM 生成更加详细的表描述
        """
        
        dialect = self.sql_database.dialect
        dialect_knowledge = get_dialect_knowledge(dialect)

        # 调用 LLM 生成详细描述
        llm_prompt = f"""
        请为以下数据库表生成中文描述。

        要求：
        1. 使用中文描述。
        2. 严禁修改、翻译或编造原始表结构中的任何表名、列名和类型信息，必须保持原样。
        3. 描述必须简洁明了，突出表的核心业务含义和数据特点，不要冗长。
        4. 重点说明表的作用、主键关系以及关键字段的业务含义。
        5. 如果你对该表结构非常熟悉且有十足把握，请提供 1-3 个高质量的“自然语言查询 -> SQL语句”示例。
           - 示例必须准确无误。
           - 格式：Q: [查询问题] A: [SQL语句]
           - 注意: 如果没有十足把握，请不要提供任何示例!!!
           - 一定注意: 如果生成示例, 必须是查询语句, 即 SELECT ...
           - 参考以下{dialect}数据库知识：
             {dialect_knowledge} 

        原始表信息：
        {description}
        """
        response = self.llm.complete(llm_prompt)
        detailed_description = response.text

        return detailed_description 