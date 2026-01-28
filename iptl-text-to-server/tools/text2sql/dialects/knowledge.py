
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
   
6. **分组与聚合规则** (GROUP BY)
   - 严格性：SELECT 中出现的非聚合列（即没有套在 SUM, COUNT 等函数里的列），必须出现在 GROUP BY 子句中。
   - 窗口函数：若要在不减少行数的情况下计算总数，使用 COUNT(*) OVER()。

8. **数组与列表处理**
   - 展开列表：如果某列存储了逗号分隔的字符串（如 'a,b,c'），使用 unnest(string_to_array(col, ',')) 将其转为多行。
   - 匹配列表：使用 ANY 操作符。例如 WHERE '2.0' = ANY(string_to_array(col, ','))。
"""

MYSQL_DIALECT_KNOWLEDGE = """
MySQL 语法知识, 生成的 SQL 语句务必严格按照下面的语法!!!

1. **标识符引用**：
   - 推荐对表名和列名使用反引号 ` 进行包裹。例如 `SELECT `column_name` FROM `table_name``。
   - 理由：MySQL 默认标识符不区分大小写（Windows/macOS），但在 Linux 上区分大小写。使用反引号可确保兼容性和避免关键字冲突。
   - 字符串字面量（值）必须使用单引号 `'`。例如 `WHERE `LANDTYPE` = '水田'`。
   - 注意：双引号 `"` 在默认 SQL 模式下也可用于字符串，但标准做法是用单引号。

2. **字符串匹配**：
   - 字符串字面量使用单引号 `'`。例如 `name = 'Alice'`。
   - 大小写敏感匹配：使用 `BINARY` 关键字或 `COLLATE` 子句。例如 `name = BINARY 'Alice'` 或 `name COLLATE utf8mb4_bin = 'Alice'`。
   - 大小写不敏感匹配：默认使用 `=` 或 `LIKE`（取决于字符集排序规则）。例如 `name LIKE '%alice%'`。
   - 正则匹配：使用 `REGEXP` 或 `RLIKE`。例如 `name REGEXP '^[A-Z]'`。

3. **日期和时间**：
   - 获取当前时间：`NOW()` 或 `CURRENT_TIMESTAMP` 或 `SYSDATE()`。
   - 日期格式化：`DATE_FORMAT(date_col, '%Y-%m-%d')`。
   - 提取部分日期：`YEAR(date_col)`, `MONTH(date_col)`, `DAY(date_col)`, `HOUR(datetime_col)` 等。
   - 时间间隔运算：
     - 加法：`DATE_ADD(date_col, INTERVAL 1 DAY)` 或 `date_col + INTERVAL 1 DAY`。
     - 减法：`DATE_SUB(date_col, INTERVAL 1 MONTH)` 或 `date_col - INTERVAL 1 HOUR`。
   - 日期截断：`DATE(datetime_col)` 截取日期部分。

4. **LIMIT 和 OFFSET**：
   - 使用 `LIMIT n` 限制返回行数。
   - 使用 `OFFSET m` 跳过前 m 行，或使用 `LIMIT m, n` 语法（跳过 m 行，返回 n 行）。
   - 示例：`LIMIT 5 OFFSET 10` 或 `LIMIT 10, 5`。

5. **常用函数**：
   - 字符串拼接：使用 `CONCAT()` 函数。例如 `CONCAT(first_name, ' ', last_name)`。
   - 聚合函数：`COUNT()`, `SUM()`, `AVG()`, `MAX()`, `MIN()`。
   - 条件表达式：`CASE WHEN condition THEN result ELSE other_result END` 或 `IF(condition, true_value, false_value)`。
   - 类型转换：`CAST(value AS type)` 或 `CONVERT(value, type)`。例如 `CAST('123' AS SIGNED)` 或 `CONVERT('123', SIGNED)`。
   - 空值处理：`IFNULL(col, default_value)` 或 `COALESCE(col1, col2, default_value)`。

6. **分组与聚合规则** (GROUP BY)：
   - 严格性：默认情况下（MySQL 5.7.5+ 启用 `ONLY_FULL_GROUP_BY` 模式），SELECT 中出现的非聚合列必须出现在 GROUP BY 子句中。
   - 宽松模式：若禁用 `ONLY_FULL_GROUP_BY`，可以在 SELECT 中使用未分组的列，但结果不确定。
   - 窗口函数（MySQL 8.0+）：若要在不减少行数的情况下计算总数，使用 `COUNT(*) OVER()`。

7. **窗口函数**（MySQL 8.0+）：
   - 排名函数：`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`。
   - 聚合窗口：`SUM(col) OVER(PARTITION BY group_col ORDER BY order_col)`。
   - 示例：`SELECT *, ROW_NUMBER() OVER(ORDER BY salary DESC) AS rank FROM employees`。

8. **数组与列表处理**：
   - MySQL 没有原生数组类型，通常使用逗号分隔的字符串。
   - 展开列表：MySQL 8.0+ 可使用递归 CTE 或 JSON 函数。对于简单情况，使用 `FIND_IN_SET()` 函数。
     - 示例：`WHERE FIND_IN_SET('value', comma_separated_col) > 0`。
   - 匹配列表中的值：
     - 使用 `FIND_IN_SET()`：`WHERE FIND_IN_SET('2.0', col)`。
     - 使用 `LIKE`：`WHERE col LIKE '%2.0%'`（注意：可能误匹配，如 '12.0'）。
   - JSON 处理（MySQL 5.7+）：
     - 提取值：`JSON_EXTRACT(json_col, '$.key')` 或 `json_col->>'$.key'`。
     - 数组展开：`JSON_TABLE()` 函数将 JSON 数组转为行。

9. **特殊操作符与语法**：
   - NULL 安全比较：使用 `<=>` 操作符。例如 `col <=> NULL` 等价于 `col IS NULL`。
   - 去重：`SELECT DISTINCT col FROM table` 或 `GROUP BY col`。
   - 排序：`ORDER BY col ASC/DESC`。多列排序：`ORDER BY col1 ASC, col2 DESC`。
   - 联合查询：`UNION`（去重）或 `UNION ALL`（保留重复）。

10. **注意事项**：
    - MySQL 默认使用 `utf8mb4` 字符集（推荐），确保存储 emoji 等 4 字节字符。
    - 存储引擎：InnoDB（默认，支持事务）和 MyISAM（不支持事务）的行为不同。
    - 版本差异：MySQL 5.x 和 8.0+ 在功能上有显著差异（如窗口函数、CTE 等仅在 8.0+ 可用）。
    - 大小写敏感：表名在 Linux 上区分大小写，在 Windows/macOS 上不区分。列名始终不区分大小写（除非使用 BINARY）。
"""

DIALECT_KNOWLEDGE_MAP = {
    "postgresql": POSTGRESQL_DIALECT_KNOWLEDGE,
    "postgres": POSTGRESQL_DIALECT_KNOWLEDGE,  # Alias
    "mysql": MYSQL_DIALECT_KNOWLEDGE,
}
