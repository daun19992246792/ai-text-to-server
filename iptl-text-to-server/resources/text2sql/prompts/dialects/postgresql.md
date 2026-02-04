PostgreSQL 语法规范，必须严格遵守。

1. **标识符**：表名/列名必须用双引号 `"`。字符串值用单引号 `'`。例如 `WHERE "name" = 'Alice'`。
2. **类型**：布尔值必须用 `TRUE`/`FALSE`，禁止用 `1`/`0`。数字比较需显式转换：`"id" = '123'::INTEGER`。
3. **字符串**：不区分大小写用 `ILIKE`；不等于用 `<>`。
4. **日期**：当前时间 `NOW()`；截断 `DATE_TRUNC('day', col)`；提取 `EXTRACT(YEAR FROM col)`；间隔 `col - INTERVAL '1 day'`。
5. **分页**：`LIMIT n OFFSET m`。
6. **函数**：拼接用 `||`；空值用 `COALESCE()`；转换用 `CAST()` 或 `::type`；`CASE WHEN ... END`。
7. **GROUP BY**：非聚合列必须在 GROUP BY 中。不减行数时用窗口函数 `COUNT(*) OVER()`。
8. **窗口函数**：`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`；`SUM(col) OVER(PARTITION BY ... ORDER BY ...)`。
9. **JSON**：提取 `col->>'key'`；包含 `col @> '{"k":"v"}'`；键存在 `col ? 'key'`。
10. **数组**：展开 `unnest(string_to_array(col, ','))`；匹配 `'val' = ANY(string_to_array(col, ','))`。
11. **禁止使用 MySQL 语法**：禁止 `1`/`0` 当布尔、`IFNULL`、`FIND_IN_SET`、反引号、`CONCAT()`。
