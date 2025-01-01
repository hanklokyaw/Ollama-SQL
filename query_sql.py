import sqlite3


def query_sqlite(database, query):
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(database)
        cursor = conn.cursor()

        # Execute the SQL query
        cursor.execute(query)

        # Fetch all results
        rows = cursor.fetchall()

        # Display the results
        for row in rows:
            print(row)

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()


# Example usage
db_file = "ana.db"
sql_query = "SELECT * FROM gems LIMIT 10;"  # Adjust the query to your needs
query_sqlite(db_file, sql_query)
