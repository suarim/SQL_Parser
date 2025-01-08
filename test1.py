import sqlglot
from sqlglot import exp, parse_one

iq=" SELECT id, status FROM users WHERE status = 'active' AND id = 7"
parsed = sqlglot.parse_one(iq)
from_part = str(parsed.args['from'])
print("from-->",from_part)
table=from_part.split()[1]
print("table-->",table)
#------------------------------------------
select_part = parsed.args['expressions']
print(select_part)
transformed_select = []
for expr in select_part:
    expr_str = str(expr)  
    transformed_select.append(expr_str)
select_part = ', '.join(transformed_select)
print(select_part)
#------------------------------------------
where_part = " ".join(str(parsed.args.get('where')) .split()[1:])
print(where_part)


import sqlglot
from sqlglot.expressions import And

def extract_and_parts(sql_query):
    """
    Extracts conditions combined by AND from the WHERE clause of a SQL query.

    Args:
        sql_query (str): The SQL query to parse.

    Returns:
        list: A list of strings representing the AND parts.
    """
    parsed_query = sqlglot.parse_one(sql_query)
    where_clause = parsed_query.find(sqlglot.expressions.Where)

    if not where_clause:
        return []

    # Recursively collect conditions connected by AND
    def collect_and_conditions(expr):
        if isinstance(expr, And):
            return collect_and_conditions(expr.args.get("this")) + collect_and_conditions(expr.args.get("expression"))
        return [expr.sql()]

    return collect_and_conditions(where_clause.this)

import sqlglot
from sqlglot import expressions as exp

sample_query = "SELECT users.id,ghj from users WHERE users.id > 5 OR orders.id > 2"

def extract_operators(expression):
    operators = []
    
    # Function to recursively extract operators
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
        elif isinstance(node, exp.GT):
            conditions = {
                'operator': '>',
                'left': str(node.left),
                'right': str(node.right)
            }
            operators.append(conditions)
        elif isinstance(node, exp.LT):
            conditions = {
                'operator': '<',
                'left': str(node.left),
                'right': str(node.right)
            }
            operators.append(conditions)
        elif isinstance(node, exp.GTE):
            conditions = {
                'operator': '>=',
                'left': str(node.left),
                'right': str(node.right)
            }
            operators.append(conditions)
        elif isinstance(node, exp.LTE):
            conditions = {
                'operator': '<=',
                'left': str(node.left),
                'right': str(node.right)
            }
            operators.append(conditions)
        elif isinstance(node, exp.EQ):
            conditions = {
                'operator': '=',
                'left': str(node.left),
                'right': str(node.right)
            }
            operators.append(conditions)
        elif isinstance(node, exp.NEQ):
            conditions = {
                'operator': '!=',
                'left': str(node.left),
                'right': str(node.right)
            }
            operators.append(conditions)
    
    traverse(expression)
    return operators

def extract_query_info(parsed_query):
    info = {
        'select_columns': [],
        'tables': [],
        'joins': [],
        'conditions': [],
        'operators': [],
        'group_by': [],
        'having': None,
        'order_by': [],
        'limit': None
    }
    
    # Extract SELECT columns
    for column in parsed_query.find_all(exp.Select):
        for expr in column.expressions:
            info['select_columns'].append(str(expr))
    
    # Extract FROM and JOIN tables
    for table in parsed_query.find_all(exp.Table):
        info['tables'].append({
            'name': table.name,
            'alias': table.alias
        })
    
    # Extract WHERE conditions and operators
    where = parsed_query.find(exp.Where)
    if where:
        info['conditions'] = [str(where.this)]
        info['operators'] = extract_operators(where.this)
    
    return info

# Parse and extract information
parsed = sqlglot.parse_one(sample_query)
query_info = extract_query_info(parsed)

# Print the results
print("\nQuery Structure Analysis:")
print("------------------------")
print("\n1. Select Columns:")
for col in query_info['select_columns']:
    print(f"  - {col}")

print("\n2. Tables:")
for table in query_info['tables']:
    print(f"  - Table: {table['name']}, Alias: {table['alias']}")

print("\n3. Where Conditions:")
for condition in query_info['conditions']:
    print(f"  - Complete condition: {condition}")

print("\n4. Operators and Their Conditions:")
for op in query_info['operators']:
    print(f"  - Operator: {op['operator']}")
    print(f"    Left: {op['left']}")
    print(f"    Right: {op['right']}")