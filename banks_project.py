# IBM Final Project
# ETL operations on the World's Largest Banks

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# Initializing the known variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

csv_path = "exchange_rate.csv"
output_path = "./Largest_banks_data.csv"

db_name = "Banks.db"
table_name = "Largest_banks"

log_file = "code_log.txt"

table_attribs = ["Name", "MC_USD_Billion"]

def log_progress(message):
    """Log a message with a timestamp to code_log.txt."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(log_file, "a", encoding="utf-8") as file:
        file.write(f"{timestamp} : {message}\n")



def extract(url, table_attribs):
    """Extract the top 10 banks by market capitalization."""

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    # Find the heading "By market capitalization"
    heading = soup.find(
        lambda tag: tag.name in ["h2", "h3", "h4"]
                    and "By market capitalization" in tag.get_text(" ", strip=True)
    )

    if heading is None:
        raise ValueError("Heading 'By market capitalization' not found.")

    table = heading.find_next("table")

    if table is None:
        raise ValueError("Market capitalization table not found.")

    rows = table.find_all("tr")
    data = []

    for row in rows[1:]:
        columns = row.find_all(["th", "td"])

        if len(columns) < 3:
            continue

        name = columns[1].get_text(" ", strip=True)
        market_cap_text = columns[2].get_text(" ", strip=True)

        market_cap_text = market_cap_text.replace(",", "")
        market_cap_text = market_cap_text.replace("\n", "").strip()

        try:
            market_cap = float(market_cap_text)
        except ValueError:
            continue

        data.append([name, market_cap])

        if len(data) == 10:
            break

    df = pd.DataFrame(data, columns=table_attribs)

    if len(df) != 10:
        raise ValueError(
            f"Expected 10 banks, extracted {len(df)}. "
            "Check the HTML table structure."
        )

    return df

def transform(df, csv_path):
    """Convert market capitalization from USD to GBP, EUR and INR."""
    # Read exchange rates from CSV
    exchange_rate_df = pd.read_csv(csv_path)

    # Convert DataFrame to dictionary
    exchange_rate = (
        exchange_rate_df
        .set_index("Currency")
        .to_dict()["Rate"]
    )

    # Convert rates to float
    gbp_rate = float(exchange_rate["GBP"])
    eur_rate = float(exchange_rate["EUR"])
    inr_rate = float(exchange_rate["INR"])

    # Add new currency columns
    df["MC_GBP_Billion"] = [
        np.round(x * gbp_rate, 2)
        for x in df["MC_USD_Billion"]
    ]

    df["MC_EUR_Billion"] = [
        np.round(x * eur_rate, 2)
        for x in df["MC_USD_Billion"]
    ]

    df["MC_INR_Billion"] = [
        np.round(x * inr_rate, 2)
        for x in df["MC_USD_Billion"]
    ]

    return df


def load_to_csv(df, output_path):
    """Save DataFrame to CSV and log the operation."""
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")


def load_to_db(df, sql_connection, table_name):
    """Save DataFrame to SQLite and log the operation."""
    df.to_sql(
        table_name,
        sql_connection,
        if_exists="replace",
        index=False
    )
    log_progress(
        "Data loaded to Database as a table, Executing queries"
    )


def run_query(query_statement, sql_connection):
    """Execute SQL query, print results and log execution."""
    print("\nSQL Query:")
    print(query_statement)

    query_output = pd.read_sql_query(
        query_statement,
        sql_connection
    )

    print("\nQuery Result:")
    print(query_output.to_string(index=False))

    log_progress(f"Executed SQL query: {query_statement}")



# Main ETL execution will be added here.

# Task 1: Initial log
log_progress("Preliminaries complete. Initiating ETL process")

# Task 2: Extraction
df = extract(url, table_attribs)

log_progress(
    "Data extraction complete. Initiating Transformation process"
)

# Task 3: Transformation
df = transform(df, csv_path)

log_progress(
    "Data transformation complete. Initiating Loading process"
)
# Task 4: Load transformed data to CSV
load_to_csv(df, output_path)


conn = sqlite3.connect(db_name)

log_progress("SQL Connection initiated")

# Load DataFrame into the database
load_to_db(df, conn, table_name)




# Query 1: Entire table
query1 = "SELECT * FROM Largest_banks"
run_query(query1, conn)

# Query 2: Average market capitalization in GBP
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query2, conn)

# Query 3: Names of the top 5 banks
query3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_query(query3, conn)

# Log completion
log_progress("Process Complete")

# Close the database connection
conn.close()
log_progress("Server Connection closed")
