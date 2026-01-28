import hashlib
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from pathlib import Path

@dataclass
class Text2SQLConfig:
    """ 配置 Text2SQL """
    db_uri: str
    include_tables: Optional[List[str]] = None
    embedding_model_name: str = 'bge-m3'
    embedding_api_key: str = ''
    embedding_api_base: str = ''
    llm_model_name: str = 'qwen3-30b-a3b-instruct-2507'
    llm_api_key: str = ''
    llm_api_base: str = ''
    # 基础路径，后续会拼接 config_id
    base_chroma_path: str = str(Path(__file__).resolve().parent / "chroma_db")
    chroma_collection_name: str = 'text2sql'
    top_k_tables: int = 5
    table_info_for_llm: bool = False    
    table_description_threads: int = 5  # 生成表schema时的线程数

    # 检索增强设置
    enable_row_retrieval: bool = True
    enable_col_retrieval: bool = True
    row_retrieval_columns: Dict[str, List[str]] = field(default_factory=dict)

    @property
    def config_id(self) -> str:
        """根据核心配置(db_uri)生成唯一指纹"""
        return hashlib.md5(self.db_uri.encode('utf-8')).hexdigest()

    @property
    def chroma_db_path(self) -> str:
        """基于配置指纹的动态向量库路径"""
        return str(Path(self.base_chroma_path) / "vectors" / self.config_id)
