import sys
import os
import json
import traceback
from sqlalchemy import text
from typing import Optional

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from tools.text2sql.engine import Text2SQLEngine
    from tools.text2sql.config import Text2SQLConfig
except ImportError as e:
    print(f"Import Error: {e}")
    print(f"sys.path: {sys.path}")
    sys.exit(1)

from tools.text_to_sql import Text2SQLService


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

    from tools.sql_executor import SQLExecutor
    from tools.sql_vaildator import SQLSecurityChecker

    service._engine = Text2SQLEngine(config)
    service._executor = SQLExecutor(service._engine.db_manager.engine)
    service._checker = SQLSecurityChecker(max_limit=50)

    return service


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

    print("Pre-executing Expected SQLs to validate dataset...")
    valid_dataset = []

    for item in dataset:
        expected_sql = item.get('expected_sql')
        if not expected_sql:
            continue

        try:
            dialect = service._engine.db_manager.sql_database.dialect
            mapping = {
                "postgresql": "postgres",
                "mysql": "mysql",
                "sqlite": "sqlite",
                "oracle": "oracle",
            }
            dialect = mapping.get(dialect.lower(), dialect)

            validated_expected_sql = service._checker.validata(expected_sql, dialect=dialect)

            expected_result_dicts = service._executor.execute(validated_expected_sql)

            cached_result = []
            if expected_result_dicts:
                for row in expected_result_dicts:
                    cached_result.append(tuple(row.values()))

            item['_expected_result_cache'] = cached_result
            valid_dataset.append(item)
        except Exception as e:
            print(f"Skipping ID {item.get('id')}: Expected SQL failed - {e}")
            item['status'] = 'Error'
            item['error'] = f"Expected SQL Execution Failed: {str(e)}"

    print(f"Valid items for prediction: {len(valid_dataset)}/{len(dataset)}")

    for item in valid_dataset:
        print(f"\nProcessing ID: {item.get('id')}")
        query = item.get('query')

        if not query:
            continue

        print(f"Query: {query}")

        try:

            print("--- Generating SQL ---")
            generated_sql_raw = service._engine.query(query)
            generated_sql_raw = str(generated_sql_raw).strip()

            if generated_sql_raw.startswith("```"):
                first_newline = generated_sql_raw.find('\n')
                if first_newline != -1:
                    last_backticks = generated_sql_raw.rfind("```")
                    if last_backticks > first_newline:
                        generated_sql_raw = generated_sql_raw[first_newline + 1:last_backticks].strip()
                    else:
                        generated_sql_raw = generated_sql_raw.strip("`").strip()
                else:
                    generated_sql_raw = generated_sql_raw.strip("`").strip()

            dialect = service._engine.db_manager.sql_database.dialect
            mapping = {
                "postgresql": "postgres",
                "mysql": "mysql",
                "sqlite": "sqlite",
                "oracle": "oracle",
            }
            dialect = mapping.get(dialect.lower(), dialect)
            validated_sql = service._checker.validata(generated_sql_raw, dialect=dialect)

            item['generated_sql'] = validated_sql
            print(f"Final SQL: {validated_sql}")

            generated_result_dicts = service._executor.execute(validated_sql)

        except Exception as e:
            print(f"Error in Text2SQL pipeline: {e}")
            item['error'] = f"Pipeline Error: {str(e)}"
            print("Status: Fail")
            continue

        try:
            expected_list = item.get('_expected_result_cache', [])

            generated_list = []
            if generated_result_dicts:
                for row in generated_result_dicts:
                    generated_list.append(tuple(row.values()))

            if len(expected_list) != len(generated_list):
                is_match = False
            else:
                is_match = True
                for exp_row, gen_row in zip(expected_list, generated_list):
                    exp_str = [str(x) for x in exp_row]
                    gen_str = [str(x) for x in gen_row]
                    if exp_str != gen_str:
                        is_match = False
                        break

            if is_match:
                print("Status: Pass")
                passed_count += 1
                item['status'] = 'Pass'
                item.pop('error', None)
            else:
                print("Status: Fail")
                print(f"Expected Result (first 5): {expected_list[:5]}")
                print(f"Generated Result (first 5): {generated_list[:5]}")
                item['status'] = 'Fail'
                item['error'] = f"Result Mismatch. Expected Len: {len(expected_list)}, Got Len: {len(generated_list)}"

        except Exception as e:
            print(f"Error executing comparison: {e}")
            print("Status: Fail")
            item['status'] = 'Fail'
            item['error'] = f"Execution Error: {str(e)}"

        item.pop('_expected_result_cache', None)

    print(f"\nEvaluation Complete. Passed: {passed_count}/{len(valid_dataset)} (Total: {len(dataset)})")

    try:
        with open(dataset_path, 'w', encoding='utf-8') as f:
            json.dump(dataset, f, ensure_ascii=False, indent=2)
        print("Results saved to eval_dataset.json")
    except Exception as e:
        print(f"Error saving results: {e}")


if __name__ == "__main__":
    evaluate()
