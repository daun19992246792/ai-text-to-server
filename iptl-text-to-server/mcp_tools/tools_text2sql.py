import mcp.types as types

TOOLS_TEXT2SQL = [
    types.Tool(
        name="text2sql",
        description="该服务提供自然语言转SQL查询功能，支持通过自然语言问题直接查询数据库中的信息。适用于需要从数据库获取精确结构化数据的场景，能够自动识别用户意图并生成标准SQL语句执行查询，返回准确的数据结果。",
        inputSchema={
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {
                    "type": "string",
                    "description": "待查询的自然语言"
                }
            }
        }
    ),
]
