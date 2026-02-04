MySQL 语法规范，必须严格遵守。

1. **标识符**：表名/列名必须用反引号 `` ` ``。字符串值用单引号 `'`。例如 `` WHERE `name` = 'Alice' ``。
2. **字符串**：大小写敏感用 `BINARY`；不敏感用 `LIKE`；正则用 `REGEXP`。
3. **日期**：当前时间 `NOW()`；格式化 `DATE_FORMAT(col, '%Y-%m-%d')`；提取 `YEAR(col)`/`MONTH(col)`/`DAY(col)`；间隔 `DATE_ADD(col, INTERVAL 1 DAY)`/`DATE_SUB()`；截取 `DATE(col)`。
4. **分页**：统一用 `LIMIT n OFFSET m`，禁止 `LIMIT m, n` 写法。
5. **函数**：拼接用 `CONCAT()`；空值用 `IFNULL()` 或 `COALESCE()`；转换用 `CAST()`；支持 `IF(condition, true_val, false_val)`。
6. **GROUP BY**：无论版本，非聚合列必须在 GROUP BY 中。不减行数时用窗口函数（8.0+）`COUNT(*) OVER()`。
7. **窗口函数**（8.0+）：`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`；`SUM(col) OVER(PARTITION BY ... ORDER BY ...)`。
8. **数组/JSON**：逗号分隔匹配用 `FIND_IN_SET('val', col)`（避免用 `LIKE` 误匹配）；JSON 提取 `JSON_EXTRACT(col, '$.key')`（5.7+）；数组展开 `JSON_TABLE()`（8.0+）。
9. **禁止使用 PostgreSQL 语法**：禁止双引号包裹标识符、`::type` 转换、`ILIKE`、`||` 拼接、`LIMIT m, n`。
