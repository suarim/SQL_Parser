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

