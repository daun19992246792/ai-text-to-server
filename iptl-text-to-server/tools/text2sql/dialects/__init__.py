from .knowledge import DIALECT_KNOWLEDGE_MAP

def get_dialect_knowledge(dialect_name: str) -> str:
    """
    根据方言名称获取对应的语法知识
    :param dialect_name: 数据库方言名称 (e.g., 'postgresql', 'mysql', 'sqlite')
    :return: 对应的语法知识字符串，如果未找到则返回空字符串
    """
    return DIALECT_KNOWLEDGE_MAP.get(dialect_name.lower(), "")
