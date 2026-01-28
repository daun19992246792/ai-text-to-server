
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

7. **空间查询** (PostGIS 常用)
   - 坐标系声明：空间计算前必须确保坐标系一致，常用 ST_SetSRID(geom, 4326) 或 ST_Transform(geom, 3857)。
   - 距离计算：
    - ST_Distance(geom1, geom2) * 111139：将度数近似转换为米。
    - 推荐：ST_DWithin(geom1, geom2, distance_in_degrees)，此函数支持索引，效率极高。
    - 最近邻检索：使用 <-> 运算符配合 ORDER BY 实现索引级的高速搜索。

8. **数组与列表处理**
   - 展开列表：如果某列存储了逗号分隔的字符串（如 'a,b,c'），使用 unnest(string_to_array(col, ',')) 将其转为多行。
   - 匹配列表：使用 ANY 操作符。例如 WHERE '2.0' = ANY(string_to_array(col, ','))。

9. **空值处理与安全**
   - 空值转换：使用 COALESCE(col, 0) 将 NULL 转换为默认值。
   - 除零保护：NULLIF(denominator, 0) 避免计算报错。
"""

DIALECT_KNOWLEDGE_MAP = {
    "postgresql": POSTGRESQL_DIALECT_KNOWLEDGE,
    "postgres": POSTGRESQL_DIALECT_KNOWLEDGE,  # Alias
}
