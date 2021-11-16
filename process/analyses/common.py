import ast


def create_simple_query(table, dic):
    def query(get, year):
        return get(table, year, dic.get(year))

    return query


def get_vars_in_expr(expr):
    st = ast.parse(expr)
    return {node.id for node in ast.walk(st) if isinstance(node, ast.Name)}
