import pandas as pd
import sqlite3
import logging

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths to CSV files and the SQLite database
file_0 = '/Users/doepesci/Desktop/forage-walmart-task-4/data/shipping_data_0.csv'
file_1 = '/Users/doepesci/Desktop/forage-walmart-task-4/data/shipping_data_1.csv'
file_2 = '/Users/doepesci/Desktop/forage-walmart-task-4/data/shipping_data_2.csv'
database_path = '/Users/doepesci/Desktop/forage-walmart-task-4/shipment_database.db'


def process_shipping_data_0(file_path, conn):
    """
    Reads data from shipping_data_0.csv and inserts it into the SQLite database.

    :param file_path: Path to the CSV file.
    :param conn: Database connection object.
    """
    try:
        df = pd.read_csv(file_path)
        df.to_sql('ShippingData0', conn, if_exists='replace', index=False)
        logging.info("Successfully processed and inserted data from shipping_data_0.csv")
    except Exception as e:
        logging.error(f"Error processing shipping_data_0.csv: {e}")


def process_shipping_data_1_and_2(file_1_path, file_2_path, conn):
    """
    Reads data from shipping_data_1.csv and shipping_data_2.csv, merges them based on 'shipment_identifier',
    and inserts the merged data into the SQLite database.

    :param file_1_path: Path to shipping_data_1.csv.
    :param file_2_path: Path to shipping_data_2.csv.
    :param conn: Database connection object.
    """
    try:
        df1 = pd.read_csv(file_1_path)
        df2 = pd.read_csv(file_2_path)

        # Merging the two dataframes based on 'shipment_identifier'
        merged_df = pd.merge(df1, df2, on='shipment_identifier')

        # Inserting the merged data into the database
        merged_df.to_sql('ShippingData1and2', conn, if_exists='replace', index=False)
        logging.info("Successfully processed and merged data from shipping_data_1.csv and shipping_data_2.csv")
    except Exception as e:
        logging.error(f"Error processing shipping_data_1.csv and shipping_data_2.csv: {e}")


def main():
    """
    Main function to run the script. Connects to the SQLite database, processes the CSV files,
    and inserts the data into the database.
    """
    try:
        conn = sqlite3.connect(database_path)
        logging.info("Connected to the SQLite database")

        process_shipping_data_0(file_0, conn)
        process_shipping_data_1_and_2(file_1, file_2, conn)

        # Closing the database connection
        conn.close()
        logging.info("Database connection closed")
    except Exception as e:
        logging.error(f"Error in main function: {e}")


# Execute the main function
if __name__ == "__main__":
    main()
