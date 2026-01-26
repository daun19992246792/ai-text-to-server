from enum import Enum

from pydantic import BaseModel


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