from enum import Enum
from mcp_tools.tools_text2sql import TOOLS_TEXT2SQL


class ToolsType(Enum):
    TEXT2SQL = TOOLS_TEXT2SQL

    def __str__(self):
        return self.name