from enum import Enum
import os
from pydantic import BaseModel, Field


def _load_default_prompt():
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # resources/config -> resources/text2sql/prompts/text_to_sql.md
        prompt_path = os.path.join(current_dir, '..', 'text2sql', 'prompts', 'text_to_sql.md')
        prompt_path = os.path.normpath(prompt_path)
        
        if os.path.exists(prompt_path):
            with open(prompt_path, 'r', encoding='utf-8') as f:
                return f.read()
    except Exception:
        pass
    return "Default Prompt Content"

_DEFAULT_PROMPT = _load_default_prompt()


class DatabaseType(Enum):
    PostgreSQL = "PostgreSQL"
    MySQL = "MySQL"
    Oracle = "Oracle"
    SQLServer  = "SQLServer "
    SQLite = "SQLite"

    def __str__(self):
        return self.name

class DatabaseConfig(BaseModel):
    host: str
    port: int = 3306
    username: str
    password: str = ""
    database_name: str
    database_type: DatabaseType

class Model(BaseModel):
    model: str
    base_url: str
    api_key: str

class ModelConfig(BaseModel):
    basic_model: Model
    embedding_model: Model

class PromptsConfig(BaseModel):
    text_to_sql_prompt: str = Field(..., json_schema_extra={"example": _DEFAULT_PROMPT}, description="Text-to-SQL Prompt Template")