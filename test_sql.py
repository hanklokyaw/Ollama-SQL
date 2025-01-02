from langchain_community.utilities import SQLDatabase

db = SQLDatabase.from_uri(f"sqlite:///ana.db")
print(db.dialect)
print(db.get_usable_table_names())
print(db.run("SELECT * FROM gems LIMIT 10;"))

