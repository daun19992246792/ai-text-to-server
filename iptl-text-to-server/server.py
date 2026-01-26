import click
import uvicorn
from fastapi import FastAPI, HTTPException

from mcp_server.mcp_server import build_mcp_server_app
from resources.config.config import DatabaseConfig, ModelConfig
from resources.config.config_save import update_global_db_config, get_global_db_config

# 初始化FastAPI应用
app = FastAPI(title="MCP Server", version="1.0")

# 新增URL接口：接收并写入数据库配置
@app.post("/api/text2/config/database", summary="更新数据库连接配置")
async def update_database_config(config: DatabaseConfig):
    """
    接收数据库连接信息并写入配置文件
    请求示例：
    POST http://localhost:8000/api/text2/config/database
    Body (JSON):
    {
        "host": "127.0.0.1",
        "port": 3306,
        "username": "root",
        "password": "123456",
        "database_name": "mcp_db"
    }
    """
    try:
        # 将Pydantic模型转为字典并写入配置文件
        update_global_db_config(config.dict())
        return {"code": 200, "message": "数据库配置写入成功", "data": config.dict()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"参数错误: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器错误: {str(e)}")

@app.get("/api/text2/config/database", summary="获取内存中的数据库连接配置")
async def get_database_config():
    config = get_global_db_config()
    if not config:
        raise HTTPException(status_code=404, detail="尚未配置数据库信息")
    return {
        "code": 200,
        "message": "获取配置成功",
        "data": config.to_dict()
    }

# 新增URL接口：接收并写入数据库配置
@app.post("/api/text2/config/model", summary="更新模型配置")
async def update_model_config(config: ModelConfig):
    """
    接收数据库连接信息并写入配置文件
    请求示例：
    POST http://localhost:8000/api/text2/config/model
    Body (JSON):
    {
        "basic_model": {
            "model": "Qwen"
            "base_url": "http://ip:8000/v1"
            "api_key": "gpustack_xxx"
        },
        "embedding_model": {
            "model": "bge-m3"
            "base_url": "http://ip:8000/v1"
            "api_key": "gpustack_xxx"
        },
    }
    """
    try:
        # 将Pydantic模型转为字典并写入配置文件
        write_model_config(config.dict())
        return {"code": 200, "message": "模型配置写入成功", "data": config.dict()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"参数错误: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器错误: {str(e)}")

app.get("/api/text2/config/model", summary="获取内存中的模型配置")
async def get_model_config():
    config = get_global_model_config()
    if not config:
        raise HTTPException(status_code=404, detail="尚未配置模型信息")
    return {
        "code": 200,
        "message": "获取配置成功",
        "data": config.to_dict()
    }

@click.command(help="启动服务")
@click.option(
    "-h"
    "--host",
    "host",
    help="host",
    default="0.0.0.0",
    type=str
)
@click.option(
    "-p",
    "--port",
    "port",
    help="port",
    default=8012
)
def main(host:str, port:int):
    app = build_mcp_server_app()
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()