from langchain_ollama import ChatOllama
import sqlite3
from tabulate import tabulate

# Initialize the LLM
MODEL_NAME = "llama3.2"
llm = ChatOllama(model=MODEL_NAME, temperature=0.0)

# Function to extract metadata from the database
def get_metadata(db_path):
    metadata = {}
    distinct_values = {}
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table_name in tables:
            table_name = table_name[0]

            # Fetch column names and data types
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [(col[1], col[2]) for col in cursor.fetchall()]

            # Fetch first 10 rows from the table
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
            rows = cursor.fetchall()

            # Fetch row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            row_count = cursor.fetchone()[0]

            # Store metadata
            metadata[table_name] = {
                "columns": columns,
                "sample_rows": rows,
                "row_count": row_count,
            }

            # Fetch distinct values for specific columns in the 'gems' table
            if table_name == "gems":
                target_columns = [
                    "Color Code", "Color Description", "Diamond Grade",
                    "Size (mm)", "Material", "Type", "Cut (Shape)"
                ]
                for column in target_columns:
                    try:
                        cursor.execute(f"SELECT DISTINCT {column} FROM {table_name};")
                        values = [row[0] for row in cursor.fetchall()]
                        distinct_values[column] = values
                    except sqlite3.Error:
                        # Skip if the column doesn't exist
                        continue

        conn.close()
    except sqlite3.Error as e:
        print(f"Error fetching metadata: {e}")
    return metadata, distinct_values

# Helper function to format metadata for the prompt
def format_metadata(metadata, distinct_values):
    formatted = []
    for table, details in metadata.items():
        formatted.append(f"Table: {table}")
        formatted.append(f"Columns: {', '.join([f'{col[0]} ({col[1]})' for col in details['columns']])}")
        formatted.append(f"Row Count: {details['row_count']}")
        formatted.append(f"Sample Rows (First 10): {details['sample_rows']}\n")

    if distinct_values:
        formatted.append("Distinct values for important columns in 'gems' table:")
        for column, values in distinct_values.items():
            formatted.append(f"{column}: {', '.join(map(str, values))}")
    return "\n".join(formatted)

# Function to generate SQL query using metadata and user input
def generate_sql_query(user_input, metadata, distinct_values):
    prompt = f"""
    The following is the structure of the SQLite database:

    {format_metadata(metadata, distinct_values)}

    The user has the following request:
    '{user_input}'

    Write a valid SQL query for the database based on the structure above. Ensure the query is efficient and syntactically correct.
    """
    response = llm.invoke(prompt)
    return response.strip()

# Function to retry and refine SQL query if needed
def retry_query(user_input, previous_query, metadata, distinct_values):
    prompt = f"""
    The initial query generated was:
    {previous_query}

    However, it did not return relevant results for the user input: '{user_input}'.
    Based on the database structure:
    {format_metadata(metadata, distinct_values)}

    Please refine the query to make it more accurate.
    """
    return llm.invoke(prompt).strip()

# Function to execute SQL query and fetch results
def execute_query(db_path, query):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        return results
    except sqlite3.OperationalError as e:
        return f"SQL Error: {e}. Please check table/column names or syntax."
    except sqlite3.Error as e:
        return f"General Error: {e}"

# Function to validate results and generate professional feedback
def validate_and_rephrase(user_input, query, results):
    if isinstance(results, str) and "Error" in results:
        feedback = f"""
        The SQL query generated was: {query}
        Unfortunately, it encountered an error:
        {results}
        Let me adjust the query and try again.
        """
        return feedback, False
    elif not results:
        feedback = f"""
        The SQL query generated was: {query}
        However, it did not return relevant results for your input: '{user_input}'.
        Let me update the query and try again.
        """
        return feedback, False
    else:
        table = tabulate(results[:10], headers="keys", tablefmt="grid") if results else "No results to display."
        feedback = f"""
        Based on your input: '{user_input}', the query executed successfully:
        {query}
        Results (sampled):
        {table}
        """
        return feedback, True

# Main app function
def main():
    # Path to your database
    db_path = "ana.db"

    # Fetch metadata and distinct values once at the start
    metadata, distinct_values = get_metadata(db_path)
    print("Database Metadata Loaded:\n", format_metadata(metadata, distinct_values))

    while True:
        user_input = input("Enter your query or 'exit' to quit: ")
        if user_input.lower() == 'exit':
            break

        # Step 1: Generate SQL query
        sql_query = generate_sql_query(user_input, metadata, distinct_values)
        print(f"Generated SQL Query:\n{sql_query}")

        # Step 2: Execute query and fetch results
        results = execute_query(db_path, sql_query)

        # Step 3: Validate results and provide feedback
        feedback, is_valid = validate_and_rephrase(user_input, sql_query, results)
        print(feedback)

        # Step 4: Retry if necessary
        if not is_valid:
            sql_query = retry_query(user_input, sql_query, metadata, distinct_values)
            print("Retrying with updated query...\n")
        else:
            print("Query successfully executed and validated.\n")

if __name__ == "__main__":
    main()
