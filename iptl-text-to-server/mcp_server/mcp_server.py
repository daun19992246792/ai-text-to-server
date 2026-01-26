import inspect
import json
from enum import Enum
from typing import get_origin, get_args

from fastapi import HTTPException
from mcp import types
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import Request
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Route, Mount

from env import THREAD_LOCAL_DATA, get_iportal_token
from mcp_tools.tools_type import ToolsType
from tool import current_locals

TOOLS_TYPES =  ["TEXT2SQL"]
class MCPServer:
    def __init__(self, tools_type):
        server = Server(tools_type)
        sse_transport = SseServerTransport("/agentx/text2/" + tools_type.lower() + "/mcp/sse/messages/")
        tools_types = ToolsType[tools_type]

        self.routes = []
        @server.list_tools()
        async def list_tools() -> list[types.Tool]:
            return tools_types.value

        @server.call_tool()
        async def call_tool(name: str, arguments: dict) -> list[
            types.TextContent | types.ImageContent | types.EmbeddedResource]:
            THREAD_LOCAL_DATA.token = arguments.get("token") or get_iportal_token()
            if "token" in arguments:
                del arguments["token"]

            # 检查函数名称是否在当前命名空间中，并且确实是一个可调用的函数
            try:
                if name in current_locals:
                    candidate = current_locals[name]

                    # 处理参数
                    arguments = await MCPServer.process_arguments(candidate, arguments)

                    # 调用工具
                    text = await MCPServer.process_candidate(candidate, arguments)
                else:
                    text = f"tool '{name}' is not found."
            except HTTPException as e:
                raise ValueError(e.detail)
            return [types.TextContent(type="text", text=text)]

        # Define MCP connection handler
        async def handle_mcp_connection(request: Request):
            async with sse_transport.connect_sse(request.scope, request.receive, request._send) as (
                    read_stream, write_stream):
                await server.run(read_stream, write_stream, server.create_initialization_options(), )
            return Response()
        self.routes.append(Route("/agentx/text2/" + tools_type.lower() + "/mcp/sse", endpoint=handle_mcp_connection))
        self.routes.append(Mount("/agentx/text2/" + tools_type.lower() + "/mcp/sse/messages/", app=sse_transport.handle_post_message), )
    def get_routes(self):
        return self.routes

    @staticmethod
    async def process_arguments(candidate, arguments):
        parameters = inspect.signature(candidate).parameters
        if len(parameters) == 1:
            annotation = next(iter(parameters.values())).annotation
            if issubclass(annotation, BaseModel):
                return annotation.parse_obj(arguments)

        for key in parameters:
            if key in arguments:
                annotation = parameters.get(key).annotation
                if annotation is not inspect.Parameter.empty:
                    origin = get_origin(annotation)
                    if origin is not None:
                        args = [arg for arg in get_args(annotation) if arg is not type(None)]
                        annotation = args[0]
                    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
                        arguments[key] = annotation.parse_obj(arguments[key])
                    elif issubclass(annotation, Enum):
                        # if not arguments[key]:
                        #     continue
                        try:
                            arguments[key] = annotation(arguments[key])
                        except KeyError as e:
                            raise HTTPException(status_code=500,
                                                detail=f"1 validation error for {key}\n  Input should be {','.join([s.name for s in annotation])} [type=enum, input_value='{arguments[key]}', input_type=str]")

        return arguments

    # 调用工具
    @staticmethod
    async def process_candidate(candidate, arguments, custom_serializer=None):
        if not callable(candidate):
            return f"'{candidate}' is not callable."

        if custom_serializer is None:
            # 如果没有提供自定义序列化器，可以使用一个简单的 lambda 函数或 None
            custom_serializer = lambda obj: obj.dict() if isinstance(obj, BaseModel) else obj

        if isinstance(arguments, BaseModel):
            # 如果 arguments 是 BaseModel 的实例，直接传递对象
            result = await candidate(arguments)
        else:
            # 否则，假设 arguments 是一个字典，使用 ** 解包
            result = await candidate(**arguments)

        # 将结果序列化为 JSON 字符串
        text = json.dumps(result, default=custom_serializer, ensure_ascii=False)
        return text
def build_mcp_server_app() -> Starlette:
    routes = []
    for tools_type in TOOLS_TYPES:
        mcp_server = MCPServer(tools_type)
        routes += mcp_server.get_routes()
    app = Starlette(
        routes=routes,
    )
    return app
