from llama_index.core.query_engine import SQLTableRetrieverQueryEngine
from llama_index.llms.openai_like import OpenAILike
from llama_index.core import Settings

from .config import Text2SQLConfig
from .db_manager import DatabaseManager
from .retriever_manager import RetrieverManager
from .prompts import TEXT_TO_SQL_PROMPT, RESPONSE_SYNTHESIS_IMPL_PROMPT
from .dialects import get_dialect_knowledge


class Text2SQLEngine:
    """
    文本转 SQL 功能的核心引擎
    负责协调数据库连接、模式提取、检索设置和查询执行
    """

    def __init__(self, config: Text2SQLConfig):
        self.config = config

        # 1.设置 LLM
        self.llm = OpenAILike(
            model=self.config.llm_model_name,
            api_key=self.config.llm_api_key,
            api_base=self.config.llm_api_base,
            context_window=32768, # 手动指定上下文窗口大小
            max_tokens=4096,  # 稍微调小一点以防超限，但保证足够
            temperature=0.1,  # 降低温度，SQL生成需要精确
            timeout=300.0,    # 增加超时时间到 300s
            reuse_client=False,
            is_chat_model=True,
        )
        Settings.llm = self.llm

        # 2.设置 Database
        print(self.config.db_uri)
        self.db_manager = DatabaseManager(
            db_uri=self.config.db_uri,
            include_tables=self.config.include_tables,
            cache_dir=self.config.chroma_db_path,
            config=self.config,
            llm=self.llm
        )

        # 3.设置 Retrievers
        self.retriever_manager = RetrieverManager(self.config, self.db_manager)
        self.table_retriever = self.retriever_manager.setup_table_retriever()
        self.rows_retrievers = self.retriever_manager.build_row_retrievers()
        self.cols_retrievers = self.retriever_manager.build_col_retrievers()

        #  构造完整的 Prompt
        dialect = self.db_manager.sql_database.dialect
        dialect_knowledge = get_dialect_knowledge(dialect)

        text_to_sql_prompt = TEXT_TO_SQL_PROMPT.partial_format(dialect_knowledge=dialect_knowledge)

        # 4.初始化查询引擎
        self.query_engine = SQLTableRetrieverQueryEngine(
            sql_database=self.db_manager.sql_database,
            table_retriever=self.table_retriever,
            # rows_retrievers=self.rows_retrievers,
            # cols_retrievers=self.cols_retrievers,
            text_to_sql_prompt=text_to_sql_prompt,
            response_synthesis_prompt=RESPONSE_SYNTHESIS_IMPL_PROMPT,
            llm=self.llm,
            synthesize_response=False, # 是否对SQL查询结果进行总结
            # verbose=True,
            sql_only=True, # 只生成SQL不执行
        )


    def query(self, query_str: str) -> str:
        """执行自然语言查询并返回结果"""

        response = self.query_engine.query(query_str)
        return str(response)

    def get_prompts(self) -> dict:
        """返回当前使用的提示词"""
        return self.query_engine.get_prompts()



