import sqlite3
import csv
import os
import shutil
from os import listdir

from apps.core.logger import Logger


class DatabaseOperation:
    """
    *****************************************************************************
    * filename:       DatabaseOperation.py
    * version:        1.0
    * author:         CODESTUDIO
    * creation date:  12/07/2025
    * description:    Class for Database Configuration to handle operations
    *****************************************************************************
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'DataOperation', mode)

    def database_connection(self, database_name):
        """Establishes connection to the SQLite database."""
        try:
            conn = sqlite3.connect(f'apps/database/{database_name}.db')
            self.logger.info(f"Opened {database_name} database successfully.")
            return conn
        except ConnectionError as ce:
            self.logger.error(f"Error while connecting to database: {ce}")
            raise

    def create_table(self, database_name, table_name, column_names):
        """Creates table if it doesn't exist and adds missing columns."""
        try:
            self.logger.info("Start of Creating Table")
            conn = self.database_connection(database_name)
            cursor = conn.cursor()

            cursor.execute(
                f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if cursor.fetchone()[0] == 1:
                self.logger.info(f"Table {table_name} already exists.")
            else:
                columns_with_types = ", ".join(
                    [f"{col} {dtype}" for col, dtype in column_names.items()]
                )
                cursor.execute(f"CREATE TABLE {table_name} ({columns_with_types});")
                self.logger.info(f"Created table {table_name} successfully.")

            conn.commit()
            conn.close()
            self.logger.info("End of Creating Table")

        except Exception as e:
            self.logger.exception(f"Exception raised while creating table: {e}")
            raise

    def insert_data(self, database_name, table_name):
        """Inserts data from CSV files into the database table."""
        try:
            conn = self.database_connection(database_name)
            good_data_path = self.data_path
            bad_data_path = self.data_path + '_rejects'

            only_files = [f for f in listdir(good_data_path)]
            self.logger.info("Start of Inserting Data into Table")

            for file in only_files:
                try:
                    with open(os.path.join(good_data_path, file), "r") as f:
                        next(f)
                        reader = csv.reader(f, delimiter=",")
                        for line in reader:
                            values = "','".join(line)
                            conn.execute(
                                f"INSERT INTO {table_name} VALUES ('{values}')")
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    self.logger.exception(
                        f"Exception raised while inserting data from file {file}: {e}")
                    shutil.move(os.path.join(good_data_path, file),
                                os.path.join(bad_data_path, file))

            conn.close()
            self.logger.info("End of Inserting Data into Table")

        except Exception as e:
            self.logger.exception(f"Exception in insert_data method: {e}")
            raise

    def export_csv(self, database_name, table_name):
        """Exports the table data to a CSV file."""
        self.file_from_db = os.path.join(self.data_path, 'validation')
        self.file_name = 'InputFile.csv'

        try:
            self.logger.info("Start of Exporting Data into CSV")
            conn = self.database_connection(database_name)
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            results = cursor.fetchall()
            headers = [description[0] for description in cursor.description]

            if not os.path.isdir(self.file_from_db):
                os.makedirs(self.file_from_db)

            with open(os.path.join(self.file_from_db, self.file_name), 'w', newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter=",")
                writer.writerow(headers)
                writer.writerows(results)

            self.logger.info("End of Exporting Data into CSV")
            conn.close()

        except Exception as e:
            self.logger.exception(f"Exception during export_csv: {e}")
            raise
