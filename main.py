import sqlglot
from sqlglot import exp, parse_one

def transform_where(where_clause, table):
    transformed_where = []
    conditions = where_clause.split('AND')
    for condition in conditions:
        condition = condition.strip()
        # If the condition contains OR, handle it separately
        if "OR" in condition:
            sub_conditions = condition.split('OR')
            transformed_sub_conditions = [(schema[table]["physical_name"] + "." + transform_condition(sub.strip(), table)) for sub in sub_conditions]
            transformed_where.append(" OR ".join(transformed_sub_conditions))
        else:
            transformed_where.append(schema[table]["physical_name"] + "." + transform_condition(condition, table))
    return " AND ".join(transformed_where)


def transform_condition(condition, table):
    # Split the condition into column, operator, and value
    parts = condition.split(maxsplit=2)
    column = parts[0]
    operator = parts[1]
    value = parts[2] if len(parts) > 2 else ""
    
    # If the column is in the schema, transform it
    if column in schema[table]["columns"]:
        column = schema[table]["columns"][column]
    
    return f"{column} {operator} {value}"


def transform_select(select_part, table):
    transformed_select = []
    for expr in select_part:
        expr_str = str(expr)
        # Check if the column exists in the schema for the table
        if expr_str in schema[table]["columns"]:
            transformed_select.append(schema[table]["physical_name"] + "." + schema[table]["columns"][expr_str])
        else:
            transformed_select.append(schema[table]["physical_name"] + "." + expr_str)
    return ' , '.join(transformed_select)


# Sample Schema
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

slugs = ["users", "orders", "items", "categories"]
input_query = "SELECT id,status FROM users WHERE status = 'active' OR id = 7"

parsed = sqlglot.parse_one(input_query)
from_part = str(parsed.args['from'])
table = from_part.split()[1]

select_part = parsed.args['expressions']
select_part_transformed = transform_select(select_part, table)
print("Transformed SELECT:", select_part_transformed)

where_part = " ".join(str(parsed.args.get('where')).split()[1:])
print("WHERE clause:", where_part)

tokens = input_query.split()
present_tables_in_query = []

for slug in slugs:
    if slug in tokens:
        present_tables_in_query.append(slug)
print("Tables in query:", present_tables_in_query)

if len(present_tables_in_query) == 1:
    table = present_tables_in_query[0]
    transformed_select = transform_select(parsed.args['expressions'], table)
    transformed_where = transform_where(where_part, table)
    transformed_query = f"SELECT {transformed_select} FROM {schema[table]['physical_name']} WHERE {schema[table]['physical_name']}.{schema[table]['columns']['id']} IN (SELECT DISTINCT {schema[table]['physical_name']}.{schema[table]['columns']['id']} AS id FROM {schema[table]['physical_name']} WHERE {transformed_where})"
    print("Final Transformed Query:", transformed_query)

