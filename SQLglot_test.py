import sqlglot
from sqlglot import expressions as exp

def extract_operators(expression):
    operators = []

    def traverse(node):
        if isinstance(node, exp.Or):
            conditions = {
                'operator': 'OR',
                'left': str(node.left),
                'right': str(node.right)
            }
            operators.append(conditions)
            traverse(node.left)
            traverse(node.right)
        elif isinstance(node, exp.And):
            conditions = {
                'operator': 'AND',
                'left': str(node.left),
                'right': str(node.right)
            }
            operators.append(conditions)
            traverse(node.left)
            traverse(node.right)
        elif isinstance(node, (exp.GT, exp.LT, exp.GTE, exp.LTE, exp.EQ, exp.NEQ)):
            operator_map = {
                exp.GT: '>', exp.LT: '<',
                exp.GTE: '>=', exp.LTE: '<=',
                exp.EQ: '=', exp.NEQ: '!='
            }
            conditions = {
                'operator': operator_map[type(node)],
                'left': str(node.left),
                'right': str(node.right)
            }
            operators.append(conditions)

    traverse(expression)
    return operators

def transform_where_condition(condition, schema):
    if isinstance(condition, (exp.GT, exp.LT, exp.GTE, exp.LTE, exp.EQ, exp.NEQ)):
        left = condition.left
        right = condition.right

        # Define operator mapping
        operator_map = {
            exp.GT: '>',
            exp.LT: '<',
            exp.GTE: '>=',
            exp.LTE: '<=',
            exp.EQ: '=',
            exp.NEQ: '!='
        }

        # Get the correct operator
        operator = operator_map[type(condition)]

        # Transform left side
        if isinstance(left, exp.Column):
            table_name = left.table
            column_name = left.name
            if table_name and column_name in schema[table_name]["columns"]:
                left_transformed = f"{schema[table_name]['physical_name']}.{schema[table_name]['columns'][column_name]}"
            else:
                left_transformed = str(left)
        else:
            left_transformed = str(left)

        # Transform right side
        if isinstance(right, exp.Column):
            table_name = right.table
            column_name = right.name
            if table_name and column_name in schema[table_name]["columns"]:
                right_transformed = f"{schema[table_name]['physical_name']}.{schema[table_name]['columns'][column_name]}"
            else:
                right_transformed = str(right)
        else:
            right_transformed = str(right)

        return f"{left_transformed} {operator} {right_transformed}"

    return str(condition)

def transform_where_multiple_tables(where_expr, schema):
    if not where_expr:
        return ""

    def transform_expression(expr):
        if isinstance(expr, exp.Or):
            left = transform_expression(expr.left)
            right = transform_expression(expr.right)
            return f"({left} OR {right})"
        elif isinstance(expr, exp.And):
            left = transform_expression(expr.left)
            right = transform_expression(expr.right)
            return f"({left} AND {right})"
        else:
            return transform_where_condition(expr, schema)

    return transform_expression(where_expr)

def transform_select(select_expressions, table, schema):
    transformed_select = []
    for expr in select_expressions:
        if isinstance(expr, exp.Column):
            table_name = expr.table or table
            column_name = expr.name
            if column_name in schema[table_name]["columns"]:
                transformed_select.append(
                    f"{schema[table_name]['physical_name']}.{schema[table_name]['columns'][column_name]}"
                )
            else:
                transformed_select.append(f"{schema[table_name]['physical_name']}.{column_name}")
    return ", ".join(transformed_select)

def get_tables_from_query(input_query,parsed_query):
    slugs = ["users", "orders", "items", "categories"]
    tokens=input_query.split()
    print("Tokens----------->",tokens)
    tables = []
    for slug in slugs:
        for token in tokens:
            if slug in token:
                if slug in tables:
                    break
                print(f"{slug} is present in query")
                tables.append(slug)


    for table in parsed_query.find_all(exp.Table):
        if table.name not in tables:
            tables.append(table.name)
    return tables

def transform_query(input_query, schema, hierarchy):
    parsed = sqlglot.parse_one(input_query)
    slugs = ["users", "orders", "items", "categories"]
    # Extract tables
    present_tables = get_tables_from_query(input_query,parsed)
    print("pt _____________>",present_tables)
    # Extract SELECT expressions
    select_expressions = parsed.args['expressions']

    # Extract WHERE clause
    where_clause = parsed.find(exp.Where)
    where_expr = where_clause.this if where_clause else None

    if len(present_tables) == 1:
        table = present_tables[0]
        transformed_select = transform_select(select_expressions, table, schema)

        if not where_expr:
            return f"SELECT {transformed_select} FROM {schema[table]['physical_name']}"

        transformed_where = transform_where_multiple_tables(where_expr, schema)
        return f"""
        SELECT {transformed_select} 
        FROM {schema[table]['physical_name']} 
        WHERE {schema[table]['physical_name']}.{schema[table]['columns']['id']} IN (
            SELECT DISTINCT {schema[table]['physical_name']}.{schema[table]['columns']['id']} AS id 
            FROM {schema[table]['physical_name']} 
            WHERE {transformed_where}
        )"""

    elif len(present_tables) == 2:
        if abs(hierarchy[present_tables[0]] - hierarchy[present_tables[1]]) == 1:
            transformed_select = transform_select(select_expressions, present_tables[0], schema)
            transformed_where = transform_where_multiple_tables(where_expr, schema)

            return f"""
            SELECT {transformed_select} 
            FROM {schema[present_tables[0]]['physical_name']} 
            WHERE {schema[present_tables[0]]['physical_name']}.{schema[present_tables[0]]['columns']['id']} IN (
                SELECT DISTINCT {schema[present_tables[0]]['physical_name']}.{schema[present_tables[0]]['columns']['id']} AS id 
                FROM {schema[present_tables[0]]['physical_name']} 
                JOIN {schema[present_tables[1]]['physical_name']} 
                    ON {schema[present_tables[1]]['physical_name']}.user_id = {schema[present_tables[0]]['physical_name']}.id 
                WHERE {transformed_where}
            )"""


schema = {
    "users": {
        "physical_name": "user_master",
        "is_public": False,
        "columns": {
            "id": "user_id",
            "spending": "total_spend",
            "budget": "spend_limit",
            "status": "user_status",
            "name": "user_name",
            "orders": {
                "relation": {"object": "orders", "column": "user_id", "virtual": True},
            },
        },
    },
    "orders": {
        "physical_name": "order_details",
        "is_public": True,
        "columns": {
            "id": "order_id",
            "amount": "order_amount",
            "status": "order_status",
            "user_id": {
                "relation": {"object": "users", "column": "id", "virtual": False},
            },
            "items": {
                "relation": {"object": "items", "column": "order_id", "virtual": True},
            },
        },
    },
    "items": {
        "physical_name": "order_item_details",
        "is_public": True,
        "columns": {
            "id": "item_id",
            "order_id": {
                "relation": {"object": "orders", "column": "id", "virtual": False},
            },
            "product_id": "product_id",
            "quantity": "item_quantity",
            "category": {
                "relation": {"object": "categories", "column": "id", "virtual": False},
            },
        },
    },
    "categories": {
        "physical_name": "category_master",
        "is_public": True,
        "columns": {
            "id": "category_id",
            "name": "category_name",
            "items": {
                "relation": {"object": "items", "column": "category", "virtual": True},
            },
        },
    },
}

heirarchy = {
    "users": 1,
    "orders": 2,
    "items": 3,
    "categories": 4
}
def main():
    # [Schema and hierarchy definitions remain the same]

    input_query = "SELECT users.id from users WHERE users.id > 5 AND orders.id > 2 OR orders.status = 'active'"

    print("Input Query:", input_query)
    try:
        transformed = transform_query(input_query, schema, heirarchy)
        print("Transformed Query:", transformed)
    except Exception as e:
        print(f"Error transforming query: {str(e)}")

if __name__ == "__main__":
    main()
