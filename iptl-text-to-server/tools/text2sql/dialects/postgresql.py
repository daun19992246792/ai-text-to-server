
POSTGRESQL_DIALECT_KNOWLEDGE = """
PostgreSQL 语法知识, 生成的 SQL 语句务必严格按照下面的语法!!!
1. **标识符引用**：
   - 必须对所有表名和列名使用双引号 `"` 进行包裹。
   - 理由：PostgreSQL 默认会将未加引号的标识符转换为小写，强制使用双引号可确保与原始表结构完全匹配。
   - 字符串字面量（值）必须使用单引号 `'`。例如 `WHERE "LANDTYPE" = '水田'`。

2. **字符串匹配**：
   - 字符串字面量使用单引号 `'`。例如 `name = 'Alice'`。
   - 大小写敏感匹配使用 `=` 或 `LIKE`。
   - 大小写不敏感匹配使用 `ILIKE`。例如 `name ILIKE '%alice%'`。

3. **日期和时间**：
   - 获取当前时间：`NOW()` 或 `CURRENT_TIMESTAMP`。
   - 日期截断：`DATE_TRUNC('day', timestamp_col)`。
   - 提取部分日期：`EXTRACT(YEAR FROM timestamp_col)`。
   - 时间间隔运算：`timestamp_col - INTERVAL '1 day'`。

4. **LIMIT 和 OFFSET**：
   - 使用 `LIMIT n` 限制返回行数。
   - 使用 `OFFSET m` 跳过前 m 行。
   - 示例：`LIMIT 5 OFFSET 10`。

5. **常用函数**：
   - 字符串拼接：使用 `||` 操作符。例如 `first_name || ' ' || last_name`。
   - 聚合函数：`COUNT()`, `SUM()`, `AVG()`, `MAX()`, `MIN()`。
   - 条件表达式：`CASE WHEN condition THEN result ELSE other_result END`。
   - 类型转换：`CAST(value AS type)` 或 `value::type`。例如 `'123'::INTEGER`。

6. **JSON 处理** (如果涉及 JSONB)：
   - 提取 JSON 字段：`json_col ->> 'key'` (返回文本)。
   - JSON 包含：`json_col @> '{"key": "value"}'`。
"""

DIALECT_KNOWLEDGE_MAP = {
    "postgresql": POSTGRESQL_DIALECT_KNOWLEDGE,
    "postgres": POSTGRESQL_DIALECT_KNOWLEDGE,  # Alias
}
