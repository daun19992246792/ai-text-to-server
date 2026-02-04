from fastapi import HTTPException, BackgroundTasks

from mcp_server.mcp_server import build_mcp_server_app
from resources.config.config import DatabaseConfig, ModelConfig, PromptsConfig
from resources.config.config_save import update_global_db_config, get_global_db_config, update_global_model_config, \
    get_global_model_config, update_global_prompts_config, get_global_prompts_config
from resources.text2sql.factory import warm_up_engine_task
import click
import uvicorn
from fastapi import FastAPI
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html
)
from fastapi.staticfiles import StaticFiles
from starlette.responses import RedirectResponse
from pathlib import Path

# 初始化FastAPI应用
app = FastAPI(
    debug=True,
    docs_url=None,
    redoc_url=None,
    title="Text2SQL"
)
app.mount('/static-doc', StaticFiles(directory=Path(__file__).resolve().parent / "static-doc", html=True),
          name='static-doc')


@app.get("/", include_in_schema=False)
async def index():
    return RedirectResponse(url='/docs')


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="/static-doc/swagger-ui-bundle.js",
        swagger_css_url="/static-doc/swagger-ui.css",
        swagger_favicon_url="/static-doc/favicon.png"
    )


@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    return get_redoc_html(
        openapi_url=app.openapi_url,
        title=app.title + " - ReDoc",
        redoc_js_url="/static-doc/redoc.standalone.js",
        redoc_favicon_url="/static-doc/favicon.png",
        with_google_fonts=False
    )

# 新增URL接口：接收并写入数据库配置
@app.post("/api/text2/config/database", summary="更新数据库连接配置")
async def update_database_config(config: DatabaseConfig, background_tasks: BackgroundTasks):
    """
    接收数据库连接信息并写入配置文件，并触发异步预热
    """
    try:
        # 将Pydantic模型转为字典并写入配置文件
        update_global_db_config(config.dict())
        
        # 触发异步预热
        background_tasks.add_task(warm_up_engine_task)
        
        return {"code": 200, "message": "数据库配置写入成功，正在后台初始化引擎...", "data": config.dict()}
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
async def update_model_config(config: ModelConfig, background_tasks: BackgroundTasks):
    """
    接收模型配置并写入配置文件，并触发异步预热（如果数据库已配置）
    """
    try:
        # 将Pydantic模型转为字典并写入配置文件
        update_global_model_config(config.dict())
        
        # 尝试触发预热（如果DB也配置了的话）
        background_tasks.add_task(warm_up_engine_task)
        
        return {"code": 200, "message": "模型配置写入成功", "data": config.dict()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"参数错误: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器错误: {str(e)}")

@app.get("/api/text2/config/model", summary="获取内存中的模型配置")
async def get_model_config():
    config = get_global_model_config()
    if not config:
        raise HTTPException(status_code=404, detail="尚未配置模型信息")
    return {
        "code": 200,
        "message": "获取配置成功",
        "data": config.to_dict()
    }

@app.get("/api/text2/config/prompts", summary="获取模型提示词配置")
async def get_prompts_config():
    config = get_global_prompts_config()
    if config:
        data = config.to_dict()
    else:
        prompt_path = Path(__file__).resolve().parent / "resources" / "text2sql" / "prompts" / "text_to_sql.md"
        try:
            TEXT_TO_SQL_TMPL = prompt_path.read_text(encoding="utf-8")
        except Exception:
            TEXT_TO_SQL_TMPL = ""
        data = {
            "text_to_sql_prompt": TEXT_TO_SQL_TMPL
        }

    return {
        "code": 200,
        "message": "获取配置成功",
        "data": data
    }

@app.post("/api/text2/config/prompts", summary="更新模型提示词配置")
async def update_prompts_config(config: PromptsConfig, background_tasks: BackgroundTasks):
    """
    接收模型提示词配置并写入配置文件，并触发异步预热
    """
    try:
        # 将Pydantic模型转为字典并写入配置文件
        update_global_prompts_config(config.dict())
        
        # 触发异步预热
        background_tasks.add_task(warm_up_engine_task)
        
        return {"code": 200, "message": "模型提示词配置写入成功", "data": config.dict()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"参数错误: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器错误: {str(e)}")

@click.command(help="启动服务")
@click.option(
    "-h",
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
    mcp_app = build_mcp_server_app(app)
    uvicorn.run(mcp_app, host=host, port=port)


if __name__ == "__main__":
    main()
