import sys
import os
import json
import traceback
from typing import Optional, Dict, Any, List

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from resources.text2sql.engine import Text2SQLEngine
    from resources.text2sql.config import Text2SQLConfig
    from resources.text2sql.service import Text2SQLService
    from resources.text2sql.sql_executor import SQLExecutor
    from resources.text2sql.sql_validator import SQLSecurityChecker
except ImportError as e:
    print(f"Import Error: {e}")
    print(f"sys.path: {sys.path}")
    sys.exit(1)


def get_service() -> Text2SQLService:
    # TODO: Please fill in your database configuration here
    db_type = ""
    db_user = ""
    db_password = ""
    db_host = ""
    db_port = ""
    db_name = ""

    # TODO: Please fill in your Model configuration here
    llm_model_name = ""
    llm_api_key = ""
    llm_api_base = ""

    embedding_model_name = ""
    embedding_api_key = ""
    embedding_api_base = ""

    if db_type == "sqlite":
        db_uri = f"sqlite:///{db_name}"
    else:
        driver = "postgresql+psycopg2" if db_type == "postgresql" else "mysql+pymysql"
        db_uri = f"{driver}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    print(f"Using DB URI: {db_uri}")

    config = Text2SQLConfig(db_uri=db_uri)

    if llm_model_name:
        config.llm_model_name = llm_model_name
    if llm_api_key:
        config.llm_api_key = llm_api_key
    if llm_api_base:
        config.llm_api_base = llm_api_base

    if embedding_model_name:
        config.embedding_model_name = embedding_model_name
    if embedding_api_key:
        config.embedding_api_key = embedding_api_key
    if embedding_api_base:
        config.embedding_api_base = embedding_api_base

    service = Text2SQLService()

    service._engine = Text2SQLEngine(config)
    service._executor = SQLExecutor(service._engine.db_manager.engine)
    service._checker = SQLSecurityChecker(max_limit=50)

    return service


def llm_evaluate(service: Text2SQLService, query: str, validated_sql: str, result: List[Dict[str, Any]]) -> bool:
    """
    使用 LLM 评估 SQL 执行结果是否正确回答了用户问题。
    """
    result_str = str(result)
    # 如果结果太长，进行截断以避免超出上下文限制
    if len(result_str) > 2000:
        result_str = result_str[:2000] + "... (truncated)"

    prompt = f"""你是一位严谨的 SQL 审计专家。你的唯一职责是判断生成的 SQL 及其执行结果是否完全、正确地回答了用户的问题。 
 
 【输入信息】 
 - 用户问题：{query} 
 - 生成的 SQL：{validated_sql} 
 - 执行结果：{result_str} 
 
 【评估维度（所有维度必须同时满足才可返回 True）】 
 
 1. 语法合规性 
    - SQL 是否语法正确，符合目标数据库方言规范？ 
    - 执行结果中是否包含语法错误、未知字段等报错信息？若有，直接返回 False。 
 
 2. 逻辑一致性 
    - SQL 是否准确实现了问题中的所有约束？ 
    - 涉及的表、字段、JOIN 关系、过滤条件、聚合方式、GROUP BY / HAVING、ORDER BY、LIMIT 等是否均正确？ 
    - 任何逻辑偏差（错误条件、缺失过滤、错误关联导致数据倍增等）均视为 False。 
 
 3. 结果可信度 
    - 执行结果的数据类型、数值范围是否符合预期和常理？ 
    - 若问题期望有数据但结果为空（且无明确的"无匹配记录"理由），返回 False。 
    - 若结果包含不相关、不完整或多余的数据，返回 False。 
 
 4. 边界与特殊情况处理 
    - 是否正确处理了 NULL 值、重复数据、大小写敏感、日期范围及边界条件？ 
 
 5. 完整性 
    - 结果是否包含了问题要求的全部信息（如所需字段、正确的排序顺序、正确的数量限制）？ 
    - 对于聚合类问题（COUNT / SUM / AVG），数值逻辑是否准确？ 
    - 对于排名 / Top-N 类问题，ORDER BY 和 LIMIT 是否正确使用？ 
 
 【特殊规则】 
 - 结果为空仅在问题明确询问不存在的实体时才可接受。 
 - SQL 执行成功但逻辑错误 → False 
 - SQL 执行成功但结果不合理 → False 
 - SQL 语法错误或执行失败 → False 
 - 所有维度均满足 → True 
 
 【输出要求】 
 请在内部深思熟虑后，仅返回 'True' 或 'False'。严禁输出任何其他解释、标点或格式。"""

    try:
        # 使用 Text2SQLEngine 中的 LLM 进行评估
        response = service._engine.llm.complete(prompt)
        content = response.text.strip().lower()
        print(f"LLM Evaluation: {content}")
        if "true" in content:
            return True
        else:
            return False
    except Exception as e:
        print(f"LLM Evaluation Failed: {e}")
        return False


def evaluate():
    dataset_path = os.path.join(current_dir, 'eval_dataset.json')
    if not os.path.exists(dataset_path):
        print(f"Error: Dataset not found at {dataset_path}")
        return

    with open(dataset_path, 'r', encoding='utf-8') as f:
        dataset = json.load(f)

    try:
        service = get_service()
    except Exception as e:
        print(f"Error initializing service or database: {e}")
        traceback.print_exc()
        return

    print(f"Starting evaluation on {len(dataset)} items...")

    passed_count = 0
    processed_dataset = []

    for item in dataset:
        print(item['id'])
        query = item.get("query")
        print(f"\nProcessing Query: {query}")

        try:
            # 1. 生成 SQL
            generated_sql = service._engine.query(query)
            
            # 2. 验证 SQL
            dialect = service._engine.db_manager.sql_database.dialect
            mapping = {
                "postgresql": "postgres",
                "mysql": "mysql",
                "sqlite": "sqlite",
                "oracle": "oracle",
            }
            dialect = mapping.get(dialect.lower(), dialect)
            validated_sql = service._checker.validata(generated_sql, dialect=dialect)
            
            # 3. 执行 SQL
            results = service._executor.execute(validated_sql)
            
            # 4. LLM 评估
            is_pass = llm_evaluate(service, query, validated_sql, results)
            
            status = "Pass" if is_pass else "Fail"
            if is_pass:
                passed_count += 1
            
            # 更新结果
            item["generated_sql"] = validated_sql
            item["status"] = status
            if not is_pass:
                # 可以选择是否记录执行结果作为错误信息的一部分，或者仅标记失败
                # item["error"] = f"LLM Evaluated as False. Results: {str(results)[:200]}..." 
                pass
            else:
                if "error" in item:
                    del item["error"]

        except Exception as e:
            print(f"Error processing item: {e}")
            item["status"] = "Error"
            item["error"] = str(e)
        
        processed_dataset.append(item)

    print(f"\nEvaluation Completed.")
    print(f"Passed: {passed_count}/{len(dataset)}")
    print(f"Pass Rate: {passed_count / len(dataset) * 100:.2f}%")

    # 将结果写回 JSON 文件
    with open(dataset_path, 'w', encoding='utf-8') as f:
        json.dump(processed_dataset, f, ensure_ascii=False, indent=2)
    print(f"Results saved to {dataset_path}")

if __name__ == "__main__":
    evaluate()
