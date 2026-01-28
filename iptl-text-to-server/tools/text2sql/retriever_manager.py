from typing import List, Dict, Optional
import os
import json
import logging
import chromadb
from filelock import FileLock
from llama_index.core import VectorStoreIndex, Settings
from llama_index.core.objects import SQLTableSchema, SQLTableNodeMapping, ObjectIndex
from llama_index.core.retrievers import BaseRetriever
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.embeddings.openai_like import OpenAILikeEmbedding
from llama_index.core import StorageContext

from .config import Text2SQLConfig
from .db_manager import DatabaseManager
from .schema_manager import SchemaManager

logger = logging.getLogger(__name__)

class RetrieverManager:
    def __init__(self, config: Text2SQLConfig, db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager

        self.embed_model = OpenAILikeEmbedding(
            model_name=self.config.embedding_model_name,
            api_key=self.config.embedding_api_key,
            api_base=self.config.embedding_api_base
        )
        Settings.embed_model = self.embed_model

    def _schema_cache_path(self) -> str:
        return os.path.join(
            self.config.chroma_db_path,
            f"{self.config.chroma_collection_name}_schemas.json",
        )

    def _build_and_persist_schemas(self) -> List[SQLTableSchema]:
        schema_manager = SchemaManager(self.db_manager)
        schemas = schema_manager.get_table_schema()
        path = self._schema_cache_path()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        data = [{"table_name": s.table_name, "context_str": s.context_str} for s in schemas]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        return schemas

    def setup_table_retriever(self) -> BaseRetriever:
        # 确保目录存在
        os.makedirs(self.config.chroma_db_path, exist_ok=True)
        
        # 使用文件锁防止并发构建导致向量库损坏
        lock_path = os.path.join(self.config.chroma_db_path, "index_build.lock")
        lock = FileLock(lock_path)

        with lock:
            db = chromadb.PersistentClient(path=self.config.chroma_db_path)
            chromadb_collection = db.get_or_create_collection(self.config.chroma_collection_name)
            vector_store = ChromaVectorStore(chroma_collection=chromadb_collection)
            table_node_mapping = SQLTableNodeMapping(self.db_manager.sql_database)

            # 检查索引是否已存在 (通过 collection 计数或 schema 缓存文件)
            # 这里的 count() 检查是最直接的
            if chromadb_collection.count() == 0:
                logger.info(f"Initializing vector index at {self.config.chroma_db_path}...")
                schemas = self._build_and_persist_schemas()
                storage_context = StorageContext.from_defaults(vector_store=vector_store)
                obj_index = ObjectIndex.from_objects(
                    schemas,
                    table_node_mapping,
                    index_cls=VectorStoreIndex,
                    storage_context=storage_context,
                )
                logger.info("Vector index built successfully.")
            else:
                logger.info(f"Loading existing vector index from {self.config.chroma_db_path}...")
                index = VectorStoreIndex.from_vector_store(
                    vector_store,
                )
                obj_index = ObjectIndex.from_objects_and_index(
                    [], # 空列表，因为我们是从 existing index 加载
                    index,
                    table_node_mapping,
                )

        return obj_index.as_retriever(similarity_top_k=self.config.top_k_tables)


    # TODO
    def build_row_retrievers(self) -> Optional[Dict[str, BaseRetriever]]:
        """
        如果配置的话, 则为特定的列构建行检索器
        返回一个字典, 将表名映射到检索器
        """
        if not self.config.enable_row_retrieval:
            return None

        row_retrievers = {}

        # self.config.row_retriever_columns 是 Dict[str, List[str]类型
        # 例如: {'users': ["city", "country"], 'products': ["category"]}
        for table_name, columns in self.config.row_retrieval_columns.items():
            if table_name not in self.db_manager.get_table_names():
                continue

            try:
                documents = []
                from sqlalchemy import text
                # 增强健壮性：处理连接失败
                with self.db_manager.engine.connect() as conn:
                    for col in columns:
                        # 修正拼写错误 (虽然原代码看似正确，但确保是 DISTINCT)
                        query = text(f'SELECT DISTINCT {col} FROM {table_name} WHERE {col} is NOT NULL')
                        result = conn.execute(query)
                        for row in result:
                            val = str(row[0])
                            documents.append(f'{col}: {val}')


                if not documents:
                    continue

                from llama_index.core import Document
                docs = [Document(text=d) for d in documents]
                index = VectorStoreIndex.from_documents(docs, embedding_model=self.embed_model)

                row_retrievers[table_name] = index.as_retriever()

            except Exception as e:
                logger.error(f'Error building row retriever for {table_name}: {e}', exc_info=True)

        return row_retrievers if row_retrievers else None

    # TODO
    def build_col_retrievers(self) -> Optional[Dict[str, Dict[str, BaseRetriever]]]:
        return None
