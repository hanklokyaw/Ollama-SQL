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

def hardcoded_metadata(user_input):
    prompt = f"""
1. Table name: gems
2. Column names (all TEXT format): "Name", "Color Code", "Color Description", "Diamond Grade", "Size (mm)", "Material", "Type", "Cut (Shape)"
3. Top 3 sample rows:
   1-('faceted-1.0BK-ce', 'BK', 'Black', None, '1', 'Ceramic', 'Synthetic', 'Round'),
   2-('faceted-1.25BK-ce', 'BK', 'Black', None, '1.25', 'Ceramic', 'Synthetic', 'Round'),
   3-('faceted-15BK-ce', 'BK', 'Black', None, '15', 'Ceramic', 'Synthetic', 'Round')
4. Unique values from columns except "Name"
   Color Code: BK, CY, DM, EM, FV, LB, MR, MW, PA, PE, RU, SA, SY, #1, #11, #14, #17, #20, #28, #30, #32, #38, #45, #5, #52, #55, #6, #7, #8, #9, FOP, AGDI, AL, AM, AQ, BKO, BTQ, BZ, CAC, CI, CRY, DCA, DCB, DCG, DCGl, DCP, DI, DIABL, DIABLK, DIABR, DIACG, DIAPK, DIASI, DIAVS, GN, GRUQ, GT, HE, IO, JD, LAB, LPS, MA, MO, PCT, PT, PUDI, RH, RMS, RQ, RUQ, SAB, SADO, SAGR, SAO, SAP, SAPK, SAPU, SAW, SAY, SP, SQ, TE, Tou, MY, TQ, TS, TZ, AB, AR, AY, BR, CB, CH, CS, CZ, DIAAQB, DIAFGHSIge, DIALBR, DIATB, FB, FM, FP, GR, JT, LC, LI, LV, MCA, MG, PI, PR, PS, PST, PW, RC, RD, RSP, SAL, SF, SG, SLP, SN, SS, TN, WO, WS, AQB, BL, BP, BW, BZR, HY, IB, KH, LP, MYG, MYS, PB, PPY, PRD, RAF, SB, SKB, SL, VI, Vi, VIO, WH, YE
   Color Description: Black, Canary Yellow, Dusty Morganite, Emerald, Fading Violet, London Blue, Morganite, Milky White, Paradise Green, Peridot, Ruby, Sapphire, Sunrise Yellow, Teal Opal, Lime Green Opal, Blue Green Opal, White Opal, Red Opal, Yellow Opal, Orange Opal, Black Opal, Light Purple Opal, Bold Red Opal, Dark Blue Opal, Purple Opal, Hot Pink Opal, Light Blue Opal, Bubblegum Pink Opal, Light Pink Opal, Dark Pink Opal, Apple Green Diamond, Alexandrite, Amethyst, Aquamarine, Black Onyx, Black Tourmalinated Quartz, Blue Zircon, Carnelian, Citrine, Chrysoprase, Dichroic Aqua, Dichroic Blue, Dichroic Gold, Dichroic Glass, Dichroic Pink, White Diamond, Blue Diamond, Black Diamond, Brown Diamond, Diamond Cognac, Pink Diamond, White Diamond SI, White Diamond VS, Garnet, Golden Rutilated Quartz, Green Tourmaline, Hematite, Iolite, Jade, Labradorite, Lapis, Moss Agate, Moonstone, Purple Copper Turquoise, Pink Tourmaline, Purple Diamond, Rhodolite, Rainbow Moonstone, Rose Quartz, Rutilated Quartz, Blue Sapphire 1, Blue Sapphire 2, Blue Sapphire 3, Dark Orange Sapphire, Green Sapphire, Orange Sapphire, Pink Sapphire 1, Pink Sapphire 2, Pink Sapphire 3, Pink Sapphire, Pink Sapphire 4, Purple Sapphire, Purple Sapphire 3, Purple Sapphire 1, White Sahhpire, Yellow Sapphire, Spectrolite, Smoky Quartz, Tiger Eye, Tourmalinated Quartz, Mystic, Turquoise, Tsavorite, Tanzanite, Aurora Borealis, Arctic Blue, Amber Yellow, Brown, Cobalt, Champagne, Crystal, Cubic Zirconia White, Aqua Blue Diamond, White Diamond FGH SI, Light Brown Diamond, Teal Blue Diamond, Fancy Brown, Frosty Mint, Fancy Purple, Green, Jet, Light Chrome, Lilac, Lavender, Mocca, Mint Green, Pink, Primrose, Paradise Shine, Pistachio, Periwinkle, Coral, Red, Rose Peach, Sapphire Light, Sunflower, Smoke Grey, Salmon Pink, Silver Night, Silver Shade, Tangerine, White Shimmer, Aqua Blue, Blue, Baby Pink, Brandy Wine, Blazing Red, Honey, Ice Blue, Khaki, Light Pink, Mystic Green, Paraiba, Poppy, Pink Red, Rainforest, Swiss Blue, Sky blue, Salmon, Violet, Violac, White Topaz, Yellow Topaz
   Diamond Grade: None, Regular, SI, VS, FGH SI
   Size (mm): 1, 1.25, 1.5, 10, 11, 12, 13, 14, 15, 17, 18, 19.5, 2, 2.5, 20, 21, 22, 26, 29, 3, 3.5, 4, 5, 6, 7, 8, 9, 1.5x3, 3x6, 4x2, 5x5, 6x8, 24, 4x8, 5x8, 2x3, 2x4, 5x7, 7x5, 28, 3x1, 3x4, 6.5, 6x13, 7.5, 9.5, 6x3, 16
   Material: Ceramic, Opal, Diamond, Chrysoberyl, Quartz, Beryl, Zircon, Chalcedony, Tourmaline, Garnet, Hematite, Dichroite, Jade, Sanidine Feldspar, Lapis Lazuli, Feldspar, Turquoise, Olivine, Ruby, Sapphire, Pleochroic/Trichroic, Lab Created, Cubic Zirconia, Spinel, Corundum, Nano, Topaz
   Type: Synthetic, Genuine, Lab Created
   Cut (Shape): Round, Marquise, Princess, Bullet, Square, Oval, Pear, Trillion, Heart, Flatback, Tapered, Emrald, Octagon
   
   The user has the following request:
    '{user_input}'

    Write a valid SQL query for the database based for query. Ensure the query is efficient and syntactically correct.
    RETURN ONLY SQL QUERY.
"""

    response = llm.invoke(prompt)
    print("*****************************\n")
    print(response)
    print("*****************************\n\n")
    return response.content.strip()

# Function to generate SQL query using metadata and user input
def generate_sql_query(user_input, metadata, distinct_values):
    prompt = f"""
    1. You are a personal assistant of Hank Kyaw, an inventory analyst at Anatometal. 
    2. You are here to answer all the questions which ONLY related to the gems information.
    3. When user ask about size such as 3x1.5 which could also mean 1.5x3 and other dimension such as 2x4 and so on.
    4. When the user ask about a broad question provide a broader scope don't guess and pick a specific gems.
    5. The following is the meta data of the gem SQLite database:

    {format_metadata(metadata, distinct_values)}

    The user has the following request:
    '{user_input}'

    Write a valid SQL query for the database based for query. Ensure the query is efficient and syntactically correct.
    RETURN ONLY SQL QUERY.
    """
    response = llm.invoke(prompt)
    print("*****************************\n")
    print(response)
    print("*****************************\n\n")
    return response.content.strip()

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
    return llm.invoke(prompt).content.strip()

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
        sql_query = hardcoded_metadata(user_input)
        print(f"Generated SQL Query:\n{sql_query}")

        # # Step 2: Execute query and fetch results
        results = execute_query(db_path, sql_query)
        print(results)
        #
        # # Step 3: Validate results and provide feedback
        # feedback, is_valid = validate_and_rephrase(user_input, sql_query, results)
        # print(feedback)
        #
        # # Step 4: Retry if necessary
        # if not is_valid:
        #     sql_query = retry_query(user_input, sql_query, metadata, distinct_values)
        #     print("Retrying with updated query...\n")
        # else:
        #     print("Query successfully executed and validated.\n")

if __name__ == "__main__":
    main()
