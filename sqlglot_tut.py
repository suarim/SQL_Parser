from sqlglot.dialects import Dialect
import sqlglot
from sqlglot import parse_one

class CustomDialect(Dialect):
    def parse(self, expression):
        # Replace occurrences of MY_FUNC with UPPER
        expression = expression.replace("MY_FUNC", "UPPER")
        return super().parse(expression)

# Example query using a custom function
query = "SELECT MY_FUNC(name) AS transformed_name FROM table"

# Parsing using the custom dialect
parsed_query = parse_one(query, dialect=CustomDialect())

# Converting back to SQL syntax
transpiled_query = parsed_query.sql()

print("Original Query:", query)
print("Transpiled Query:", transpiled_query)
