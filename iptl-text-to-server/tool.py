from resources.text2sql.service import ToolContainer
# ================================================= 资源操作相关 start ===================================================

# 用户登录查询
async def text2sql(query: str):
    return ToolContainer.query_database(query)

current_locals=locals()