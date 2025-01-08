import sqlglot
from sqlglot import exp, parse_one


def transform_where_multiple_tables(where_clause, tables_involved):
    transformed_where = []
    conditions = where_clause.split('AND')  # Split by 'AND' for simplicity; adjust as needed
    for condition in conditions:
        condition = condition.strip()
        
        # If the condition contains OR, handle it separately
        if "OR" in condition:
            sub_conditions = condition.split('OR')
            transformed_sub_conditions = []
            for sub in sub_conditions:
                sub = sub.strip()
                for table in tables_involved:
                    if table in sub:
                        transformed_sub_conditions.append(
                            schema[table]["physical_name"] + "." + transform_condition(sub, table).split(".")[1]
                        )
                        break
            transformed_where.append(" OR ".join(transformed_sub_conditions))
        else:
            # Apply transformation for each table involved in the condition
            for table in tables_involved:
                if table in condition:
                    x=transform_condition(condition, table).split(".")[1].split()[0]
                    print("x-->",x)
                    transformed_where.append(
                        schema[table]["physical_name"] + "." + schema[table]['columns'][transform_condition(condition, table).split(".")[1].split()[0]] + " "+transform_condition(condition, table).split(".")[1].split()[1]+" "+transform_condition(condition, table).split(".")[1].split()[2]
                    )
    print("Transformed WHERE:", " AND ".join(transformed_where))
    return " AND ".join(transformed_where)

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
    table1=present_tables_in_query[0]
    table2=present_tables_in_query[1]
    select_part = parsed.args['expressions']
    print("SELECT part:", select_part)
    transformations_of_select_part_with_multiple_tables = []
    for expr in select_part:
        expr_str = str(expr)
        print("expr_str-->",expr_str)
        transformations_of_select_part_with_multiple_tables.append(schema[expr_str.split(".")[0]]['physical_name']+"."+schema[expr_str.split(".")[0]]["columns"][expr_str.split(".")[1]])
        print("transformations_of_select_part_with_multiple_tables-->",transformations_of_select_part_with_multiple_tables)
    transformed_select = ' , '.join(transformations_of_select_part_with_multiple_tables)
    print("Transformed SELECT:", transformed_select)
    transformed_where=transform_where_multiple_tables(where_part, present_tables_in_query)
    print("Transformed WHERE:", transformed_where)
    transformed_query = f"SELECT {transformed_select} FROM {schema[table1]['physical_name']} WHERE {schema[table1]['physical_name']}.{schema[table1]['columns']['id']} IN (SELECT DISTINCT {schema[table1]['physical_name']}.{schema[table1]['columns']['id']} AS id FROM {schema[table1]['physical_name']} JOIN {schema[table2]['physical_name']} ON {schema[table2]['physical_name']}.user_id = {schema[table1]['physical_name']}.id WHERE {transformed_where})"
    print("Final Transformed Query:", transformed_query)
    return transformed_query

slugs = ["users", "orders", "items", "categories"]
input_query = "SELECT users.id from users WHERE users.id=5 > orders.status='active'"
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
print(tokens)
for slug in slugs:
    for token in tokens:
        if slug in token:
            if slug in present_tables_in_query:
                break
            print(f"{slug} is present in the query")
            present_tables_in_query.append(slug)
print("Tables in query:", present_tables_in_query)

if len(present_tables_in_query) == 1:
    output_query=transform_for_single_table(present_tables_in_query, parsed, where_part, schema)
elif len(present_tables_in_query)==2:
    if abs(heirarchy[present_tables_in_query[0]]-heirarchy[present_tables_in_query[1]])==1:
        output_query=transform_for_multiple_table(present_tables_in_query, parsed, where_part, schema)



