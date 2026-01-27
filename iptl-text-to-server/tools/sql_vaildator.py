from sqlglot import exp, parse_one, parse, optimizer
from sqlglot.optimizer.simplify import simplify
from sqlglot.optimizer.canonicalize import canonicalize
from typing import Set, Optional, Any

class SQLSecurityChecker:
    """
    使用 sqlglot 的 SQL查询安全检查器
    确保 SQL 查询是只读的 SELECT 语句
    仅访问允许的表(黑名单检查), 防止 SQL 注入
    强制执行 LIMIT 子句, 并应用 SQL 优化
    最后执行验证的 SQL
    """

    def __init__(self, blocked_tables: Optional[Set[str]] = None, max_limit: int = 50):
        self.blocked_tables = {t.lower() for t in blocked_tables} if blocked_tables else set()
        self.max_limit = max_limit

        # 禁止执行可能导致远程代码执行、拒绝服务攻击、数据泄露的功能
        self.forbidden_funcs = {
            'sys_exec', 'shell_exec', 'load_file', 'sleep', 'benchmark',
            'into_outfile', 'into_dumpfile', 'user_lock', 'release_lock',
            'get_lock', 'master_pos_wait', 'waitfor', 'pg_sleep',
            'dbms_lock', 'exec', 'execute', 'xp_cmdshell', 'sp_executesql',
            'user', 'current_user', 'session_user', 'database',
            'schema', 'version', 'connection_id', 'last_insert_id',
        }

    def validata(self, sql: str, dialect: str = 'postgresql', optimize: bool = True) -> str:
        """验证并优化给定的 SQL 查询"""

        if len([e for e in parse(sql, read=dialect)]) > 1:
            raise ValueError('安全拦截: 不允许执行多个 SQL 语句')

        try:
            expression = parse_one(sql, dialect=dialect)
            # print(expression.__repr__())
        except Exception as e:
            raise ValueError(f'SQL 语法错误{str(e)}')

        if not isinstance(expression, exp.Query):
            raise ValueError('安全拦截: 仅允许 SELECT 语句(只读模式)')

        self._check_recursive(expression)

        if optimize:
            expression = self._optimize_query(expression)

        expression = self._apply_limit_optimization(expression)

        return expression.sql(dialect=dialect)

    def _check_recursive(self, node):
        """递归检查 AST 是否存在安全违规 """

        if isinstance(node, exp.Table):
            table_name = node.name.lower()

            if table_name in self.blocked_tables:
                raise ValueError(f'安全拦截: 不允许访问{table_name}表')

        if isinstance(node, exp.Func):
            func_name = node.sql_name().lower()
            if func_name in self.forbidden_funcs:
                raise ValueError(f'安全拦截: 禁止使用已知危险函数 {func_name}')

        if isinstance(node, exp.Anonymous):
            func_name = node.this.lower() if isinstance(node.this, str) else ""
            if func_name in self.forbidden_funcs:
                raise ValueError(f'安全拦截：检测到黑名单中的自定义/匿名函数 {func_name}')

        write_operations = (
            exp.Insert, exp.Update, exp.Delete, exp.Drop,
            exp.Create, exp.Alter, exp.Merge,
            # exp.Union
        )
        if isinstance(node, write_operations):
            raise ValueError(f'安全拦截: 只读模式下不允许执行{type(node).__name__}操作')

        if isinstance(node, exp.Join):
            on = node.args.get('on')
            using = node.args.get('using')
            kind = node.args.get('kind')

            if not on and not using and kind not in ('CROSS', 'NATURAL'):
                raise ValueError('安全拦截: JOIN 缺少 ON 或 USING 条件, 可能导致笛卡尔积风险')

            if isinstance(on, exp.EQ):
                if isinstance(on.left, exp.Literal) and isinstance(on.right, exp.Literal):
                    if on.left.this == on.right.this:
                        raise ValueError('安全拦截: JOIN 条件为恒等式(ON 1=1), 已拦截')

        if isinstance(node, exp.Select):
            from_expr = node.args.get('from')
            if from_expr and len(from_expr.expressions) > 1:
                raise ValueError('安全拦截: 不支持在 FROM 中使用逗号分隔多表，请使用显式 JOIN 并提供 ON 条件。')

        if isinstance(node, exp.Column):
            col_name = node.this.name.lower()

            if col_name.startswith('@@'):
                raise ValueError(f'安全拦截: 禁止访问系统变量{col_name}')

            if 'information_schema' in col_name:
                raise ValueError(f'安全拦截: 禁止访问系统元数据')


        for child in node.iter_expressions():
            self._check_recursive(child)

    def _optimize_query(self, expression):
        """对查询结构应用安全优化, 简化布尔表达式, 消除不必要的子查询和公用表表达式"""
        rules = [
            simplify,              # 简化逻辑
            # eliminate_subqueries,  # 消除子查询
            # eliminate_ctes,        # 消除 CTE (公用表达式)
            canonicalize,          # 规范化
        ]

        try:
            expression = optimizer.optimize(expression, schema=None, rules=rules)

        except Exception:
            pass

        return expression

    def _apply_limit_optimization(self, expression: exp.Select) -> exp.Select:
        """强制查询设置最大限制, 以防止检索过多数据, 保证不超过 max_limit"""

        if not isinstance(expression, exp.Query):
            raise ValueError('安全拦截: 只允许对 SELECT 做查询限制')

        current_limit = expression.args.get('limit')
        final_limit = self.max_limit

        if current_limit:
                limit_node = current_limit.this

                if isinstance(limit_node, exp.Literal) and limit_node.is_number:
                    try:
                        user_val = int(limit_node.this)
                        if user_val < self.max_limit:
                            final_limit = user_val
                    except (ValueError, TypeError):
                        pass
                elif isinstance(limit_node, (exp.Parameter, exp.Var)):
                    return expression

        return expression.limit(final_limit)

