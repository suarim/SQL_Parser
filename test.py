import sqlglot
from sqlglot import exp, parse_one

def transform_where_multiple_tables(where_clause, tables_involved):
    transformed_where = []
    comparison_operators = ['>=', '<=', '>', '<', '=']

    def transform_single_condition(condition):
        condition = condition.strip()
        for op in comparison_operators:
            if op in condition:
                left_right = condition.split(op)
                left_part = left_right[0].strip()
                right_part = left_right[1].strip()

                # Handle left side
                if '.' in left_part:
                    left_table, left_col = left_part.split('.')
                    left_transformed = f"{schema[left_table]['physical_name']}.{schema[left_table]['columns'][left_col]}"
                else:
                    left_transformed = left_part

                # Handle right side
                if '.' in right_part:
                    right_table, right_col = right_part.split('.')
                    right_transformed = f"{schema[right_table]['physical_name']}.{schema[right_table]['columns'][right_col]}"
                else:
                    # Remove any quotes from the value if present
                    right_transformed = right_part.strip("'\"")
                    # Add quotes back if it's a string value
                    if not right_transformed.isdigit():
                        right_transformed = f"'{right_transformed}'"

                return f"{left_transformed} {op} {right_transformed}"
        return condition

    # First split by OR
    or_conditions = where_clause.split(' OR ')
    or_parts = []

    for or_part in or_conditions:
        # Then split by AND within each OR part
        if ' AND ' in or_part:
            and_conditions = or_part.split(' AND ')
            transformed_and_conditions = [transform_single_condition(cond) for cond in and_conditions]
            or_parts.append('(' + ' AND '.join(transformed_and_conditions) + ')')
        else:
            or_parts.append(transform_single_condition(or_part))

    return ' OR '.join(or_parts)

def transform_where(where_clause, table):
    transformed_where = []
    comparison_operators = ['>=', '<=', '>', '<', '=']

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
    comparison_operators = ['>=', '<=', '>', '<', '=']

    # Find which operator is being used
    used_operator = None
    for op in comparison_operators:
        if op in condition:
            used_operator = op
            break

    if used_operator:
        parts = condition.split(used_operator)
        column = parts[0].strip()
        value = parts[1].strip()

        # If the column is in the schema, transform it
        if column in schema[table]["columns"]:
            column = schema[table]["columns"][column]

        return f"{column} {used_operator} {value}"

    return condition

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

def transform_for_single_table(present_tables_in_query, parsed, where_part, schema):
    print("Single Table Transformation")
    table = present_tables_in_query[0]
    transformed_select = transform_select(parsed.args['expressions'], table)
    if len(where_part) == 0:
        transformed_query = f"SELECT {transformed_select} FROM {schema[table]['physical_name']}"
        print("Final Transformed Query:", transformed_query)
        return transformed_query

    transformed_where = transform_where(where_part, table)
    transformed_query = f"SELECT {transformed_select} FROM {schema[table]['physical_name']} WHERE {schema[table]['physical_name']}.{schema[table]['columns']['id']} IN (SELECT DISTINCT {schema[table]['physical_name']}.{schema[table]['columns']['id']} AS id FROM {schema[table]['physical_name']} WHERE {transformed_where})"
    print("Final Transformed Query:", transformed_query)
    return transformed_query

def transform_for_multiple_table(present_tables_in_query, parsed, where_part, schema):
    table1 = present_tables_in_query[0]
    table2 = present_tables_in_query[1]
    select_part = parsed.args['expressions']
    transformations_of_select_part_with_multiple_tables = []

    for expr in select_part:
        expr_str = str(expr)
        transformations_of_select_part_with_multiple_tables.append(
            schema[expr_str.split(".")[0]]['physical_name'] + "." +
            schema[expr_str.split(".")[0]]["columns"][expr_str.split(".")[1]]
        )

    transformed_select = ' , '.join(transformations_of_select_part_with_multiple_tables)
    transformed_where = transform_where_multiple_tables(where_part, present_tables_in_query)

    transformed_query = f"""
   SELECT {transformed_select} 
   FROM {schema[table1]['physical_name']} 
   WHERE {schema[table1]['physical_name']}.{schema[table1]['columns']['id']} IN (
       SELECT DISTINCT {schema[table1]['physical_name']}.{schema[table1]['columns']['id']} AS id 
       FROM {schema[table1]['physical_name']} 
       JOIN {schema[table2]['physical_name']} ON {schema[table2]['physical_name']}.user_id = {schema[table1]['physical_name']}.id 
       WHERE {transformed_where}
   )"""

    print("Final Transformed Query:", transformed_query)
    return transformed_query

slugs = ["users", "orders", "items", "categories"]
input_query = "SELECT users.id from users WHERE users.id > 5  AND  orders.id > 2 OR users.status = 'active' "
output_query = ""

parsed = sqlglot.parse_one(input_query)
from_part = str(parsed.args['from'])
table = from_part.split()[1]

select_part = parsed.args['expressions']
print("SELECT part:", str(select_part))
select_part_transformed = transform_select(select_part, table)
print("Transformed SELECT:", select_part_transformed)

where_part = " ".join(str(parsed.args.get('where')).split()[1:])
print("WHERE clause:", where_part)

tokens = input_query.split()
present_tables_in_query = []

for slug in slugs:
    for token in tokens:
        if slug in token:
            if slug not in present_tables_in_query:
                print(f"{slug} is present in the query")
                present_tables_in_query.append(slug)

print("Tables in query:", present_tables_in_query)

if len(present_tables_in_query) == 1:
    output_query = transform_for_single_table(present_tables_in_query, parsed, where_part, schema)
elif len(present_tables_in_query) == 2:
    if abs(heirarchy[present_tables_in_query[0]] - heirarchy[present_tables_in_query[1]]) == 1:
        output_query = transform_for_multiple_table(present_tables_in_query, parsed, where_part, schema)