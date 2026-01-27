from tools.text_to_sql import query_database
# ================================================= 资源操作相关 start ===================================================

# 用户登录查询
async def text2sql(query: str):
    return query_database(query)

current_locals=locals()