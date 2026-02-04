from llama_index.core import PromptTemplate
import os
from resources.config.config_save import get_global_prompts_config



current_dir = os.path.dirname(os.path.abspath(__file__))
template_path = os.path.join(current_dir, 'prompts', 'text_to_sql.md')

with open(template_path, 'r', encoding='utf-8') as f:
    TEXT_TO_SQL_TMPL = f.read()

TEXT_TO_SQL_PROMPT = PromptTemplate(TEXT_TO_SQL_TMPL)

def get_text_to_sql_template() -> PromptTemplate:
    config = get_global_prompts_config()
    if config and config.text_to_sql_prompt:
        return PromptTemplate(config.text_to_sql_prompt)
    return TEXT_TO_SQL_PROMPT


# Dialect Knowledge Logic
def load_dialect_template(filename):
    path = os.path.join(current_dir, 'prompts', 'dialects', filename)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return ""

POSTGRESQL_DIALECT_KNOWLEDGE = load_dialect_template('postgresql.md')
MYSQL_DIALECT_KNOWLEDGE = load_dialect_template('mysql.md')

DIALECT_KNOWLEDGE_MAP = {
    "postgresql": POSTGRESQL_DIALECT_KNOWLEDGE,
    "postgres":   POSTGRESQL_DIALECT_KNOWLEDGE,
    "pg":         POSTGRESQL_DIALECT_KNOWLEDGE,
    "psql":       POSTGRESQL_DIALECT_KNOWLEDGE,
    "mysql":      MYSQL_DIALECT_KNOWLEDGE,
    "mariadb":    MYSQL_DIALECT_KNOWLEDGE,
}

def get_dialect_knowledge(dialect_name: str) -> str:
    """
    根据方言名称获取对应的语法知识
    :param dialect_name: 数据库方言名称 (e.g., 'postgresql', 'mysql', 'sqlite')
    :return: 对应的语法知识字符串，如果未找到则返回空字符串
    """
    return DIALECT_KNOWLEDGE_MAP.get(dialect_name.lower(), "")
