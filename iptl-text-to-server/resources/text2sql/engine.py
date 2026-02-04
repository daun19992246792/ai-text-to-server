from threading import settrace_all_threads
from llama_index.core.query_engine import SQLTableRetrieverQueryEngine
from llama_index.llms.openai_like import OpenAILike
from llama_index.embeddings.openai_like import OpenAILikeEmbedding
from llama_index.core import Settings

from .config import Text2SQLConfig
from .db_manager import DatabaseManager
from .prompts import get_text_to_sql_template, get_dialect_knowledge
from .retriever_manager import RetrieverManager
from llama_index.core import PromptTemplate




class Text2SQLEngine:
    """
    文本转 SQL 功能的核心引擎
    负责协调数据库连接、模式提取、检索设置和查询执行
    """

    def __init__(self, config: Text2SQLConfig):
        self.config = config

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

        self.db_manager = DatabaseManager(
            db_uri=self.config.db_uri,
            include_tables=self.config.include_tables,
            config=self.config,
            llm=self.llm
        )

        self.retriever_manager = RetrieverManager(self.config, self.db_manager)
        self.table_retriever = self.retriever_manager.setup_table_retriever()

        self._prompt_text = ""
        self._build_query_engine()

    def _get_prompt_text(self) -> str:
        template = get_text_to_sql_template()
        if isinstance(template, PromptTemplate):
            return template.template if hasattr(template, "template") else str(template)
        return str(template)

    def _build_text_to_sql_prompt(self):
        dialect = self.db_manager.sql_database.dialect
        dialect_knowledge = get_dialect_knowledge(dialect)
        template_str = get_text_to_sql_template()
        self._prompt_text = self._get_prompt_text()
        return template_str.partial_format(dialect_knowledge=dialect_knowledge)

    def _build_query_engine(self):
        text_to_sql_prompt = self._build_text_to_sql_prompt()
        # print(text_to_sql_prompt)
        self.query_engine = SQLTableRetrieverQueryEngine(
            sql_database=self.db_manager.sql_database,
            table_retriever=self.table_retriever,
            text_to_sql_prompt=text_to_sql_prompt,
            llm=self.llm,
            synthesize_response=False,
            sql_only=True,
        )

    def update_prompt(self):
        if self._get_prompt_text() != self._prompt_text:
            self._build_query_engine()


    def query(self, query_str: str) -> str:
        """执行自然语言查询并返回结果"""

        response = self.query_engine.query(query_str)
        return str(response)

    def get_prompts(self) -> dict:
        """返回当前使用的提示词"""
        return self.query_engine.get_prompts()

