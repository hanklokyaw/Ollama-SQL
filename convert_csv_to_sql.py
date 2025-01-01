import pandas as pd
import sqlite3


def csv_to_sqlite(csv_file, sqlite_db, table_name):
    # Load the CSV file into a DataFrame
    df = pd.read_csv(csv_file)

    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(sqlite_db)

    # Write the DataFrame to a SQLite table
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Close the connection
    conn.close()
    print(f"Data from '{csv_file}' has been imported into the '{table_name}' table of the '{sqlite_db}' database.")


# Example usage:
csv_to_sqlite("gems_db_raw.csv", "ana.db", "gems")
