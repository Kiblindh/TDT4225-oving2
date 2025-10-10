import mysql.connector
import pandas as pd
from tabulate import tabulate
import json
from DbConnector import DbConnector
import time


"""
THIS SCRIPT REPRESENTS AN ALTERNATIVE WAY TO FILL THE DATABASE, BUT IT IS NOT AS EFFICIENT AS THE MAIN SCRIPT createtables.py
IT IS LEFT HERE ONLY TO SHOWCASE THE WORK THAT HAS BEEN AND THE ALTERNATIVE APPROACH.

DONT RUN THIS SCRIPT WHEN EVALUATING!!!
"""

class CreateTables:
    def __init__(self):
        self.connection = DbConnector(HOST="tdt4225-06.idi.ntnu.no",
                 DATABASE="oving2_test",
                 USER="group6",
                 PASSWORD="group6")
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    # ----------------------------------------------------------------------
    # TABLE CREATION METHODS
    # ----------------------------------------------------------------------
    def create_trip_table(self, table_name="trip"):
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                tripId INT AUTO_INCREMENT PRIMARY KEY,
                originalTripId BIGINT NOT NULL,
                taxiId BIGINT NOT NULL,
                startTime DATETIME NOT NULL,
                dayType CHAR(1) NOT NULL,
                missingData BOOLEAN NOT NULL
            ) ENGINE=InnoDB
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_point_table(self, table_name="point"):
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                pointId INT AUTO_INCREMENT PRIMARY KEY,
                latitude DECIMAL(10, 7) NOT NULL,
                longitude DECIMAL(10, 7) NOT NULL,
                UNIQUE (latitude, longitude)
            ) ENGINE=InnoDB
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_path_table(self, table_name="path"):
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                tripId INT NOT NULL,
                pointId INT NOT NULL,
                idx INT NOT NULL,
                PRIMARY KEY (tripId, idx),
                FOREIGN KEY (tripId) REFERENCES trip(tripId) ON DELETE CASCADE,
                FOREIGN KEY (pointId) REFERENCES point(pointId) ON DELETE CASCADE
            ) ENGINE=InnoDB
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_origin_call_table(self, table_name="origin_call"):
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                tripId INT PRIMARY KEY,
                callerId INT NOT NULL,
                FOREIGN KEY (tripId) REFERENCES trip(tripId) ON DELETE CASCADE
            ) ENGINE=InnoDB
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_origin_stand_table(self, table_name="origin_stand"):
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                tripId INT PRIMARY KEY,
                standId INT NOT NULL,
                FOREIGN KEY (tripId) REFERENCES trip(tripId) ON DELETE CASCADE
            ) ENGINE=InnoDB
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    # ----------------------------------------------------------------------
    # UTILITIES
    # ----------------------------------------------------------------------
    def drop_table(self, table_name):
        print(f"Dropping table {table_name}...")
        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.db_connection.commit()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    # ----------------------------------------------------------------------
    # LOAD RAW PORTO DATA
    # ----------------------------------------------------------------------
    def load_raw_porto_data(self, csv_path):
        """
        Bulk load porto.csv into MySQL using LOAD DATA LOCAL INFILE.
        Make sure your MySQL connection allows local_infile.
        """
        cursor = self.cursor
        connection = self.db_connection
        self.cursor.execute("SET GLOBAL local_infile = 1;")
        self.db_connection.commit()

        # 1. Create staging table
        print("Creating staging table porto_raw...")
        cursor.execute("DROP TABLE IF EXISTS porto_raw")
        cursor.execute("""
            CREATE TABLE porto_raw (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                TRIP_ID      BIGINT,
                CALL_TYPE    CHAR(1) NULL,
                ORIGIN_CALL  INT NULL,
                ORIGIN_STAND INT NULL,
                TAXI_ID      BIGINT,
                `TIMESTAMP`  BIGINT,
                DAY_TYPE     CHAR(1),
                MISSING_DATA TINYINT(1),
                POLYLINE     JSON
            ) ENGINE=InnoDB
        """)
        connection.commit()

        # 2. Load data
        print(f"Loading data from {csv_path} ...")
        csv_path_fixed = csv_path.replace("\\", "/")
        load_query = f"""
            LOAD DATA LOCAL INFILE '{csv_path_fixed}'
            INTO TABLE porto_raw
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\\\'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (
            TRIP_ID,
            CALL_TYPE,
            ORIGIN_CALL,
            ORIGIN_STAND,
            TAXI_ID,
            `TIMESTAMP`,
            DAY_TYPE,
            @v_MISSING_DATA,
            @v_POLYLINE
            )
            SET
            ORIGIN_CALL = NULLIF(ORIGIN_CALL, ''),
            ORIGIN_STAND = NULLIF(ORIGIN_STAND, ''),
            MISSING_DATA = CASE
                       WHEN UPPER(TRIM(@v_MISSING_DATA)) = 'TRUE' THEN 1
                       WHEN UPPER(TRIM(@v_MISSING_DATA)) = 'FALSE' THEN 0
                       ELSE NULL
                     END,
            POLYLINE = CASE
                        WHEN JSON_VALID(@v_POLYLINE) THEN CAST(@v_POLYLINE AS JSON)
                        ELSE JSON_ARRAY()
                        END
        """
        cursor.execute(load_query)
        connection.commit()
        print("CSV successfully loaded into porto_raw.")

    # ----------------------------------------------------------------------
    # NORMALIZE RAW DATA
    # ----------------------------------------------------------------------
    def normalize_data(self):
        print("Normalizing porto_raw into normalized tables...")

        cursor = self.cursor
        connection = self.db_connection
        connection.commit()
        start = time.time()

        # Trip table
        cursor.execute("""
            INSERT IGNORE INTO trip (tripId, originalTripId, taxiId, startTime, dayType, missingData)
            SELECT
            ID, TRIP_ID, TAXI_ID, FROM_UNIXTIME(`TIMESTAMP`), DAY_TYPE, MISSING_DATA
            FROM porto_raw
        """)
        connection.commit()
        print("Inserted data into trip table.")
        print(f"⏱️ Trip table insert took {time.time() - start:.2f} seconds.")


        # Origin_call table
        cursor.execute("""
            INSERT IGNORE INTO origin_call (tripId, callerId)
            SELECT t.tripId, r.ORIGIN_CALL
            FROM porto_raw r
            JOIN trip t ON t.tripId = r.ID
            WHERE r.ORIGIN_CALL IS NOT NULL
        """)
        connection.commit()
        print("Inserted data into origin_call table.")
        print(f"⏱️ Origin_call table insert took {time.time() - start:.2f} seconds.")

        # Origin_stand table
        cursor.execute("""
            INSERT IGNORE INTO origin_stand (tripId, standId)
            SELECT t.tripId, r.ORIGIN_STAND
            FROM porto_raw r
            JOIN trip t ON t.tripId = r.ID
            WHERE r.ORIGIN_STAND IS NOT NULL
        """)
        connection.commit()
        print("Inserted data into origin_stand table.")
        print(f"⏱️ Origin_stand table insert took {time.time() - start:.2f} seconds.")


        # Extract all unique points from porto_raw and insert into point table
        cursor.execute("""
            INSERT IGNORE INTO point (latitude, longitude)
            SELECT DISTINCT
                CAST(JSON_EXTRACT(j.value, '$[1]') AS DECIMAL(10, 7)) AS latitude,
                CAST(JSON_EXTRACT(j.value, '$[0]') AS DECIMAL(10, 7)) AS longitude
            FROM porto_raw r
            JOIN JSON_TABLE(r.POLYLINE, '$[*]' COLUMNS (value JSON PATH '$')) AS j
            WHERE JSON_LENGTH(r.POLYLINE) > 0
        """)
        connection.commit()
        print("Inserted unique points into point table.")
        print(f"⏱️ Point table insert took {time.time() - start:.2f} seconds.")


        # Insert into paths
        self.cursor.execute("SELECT MIN(ID), MAX(ID) FROM porto_raw")
        min_id, max_id = self.cursor.fetchone()
        batch_size = 10000  # Adjust as needed

        for batch_start in range(min_id, max_id + 1, batch_size):
            batch_end = batch_start + batch_size - 1
            print(f"Inserting paths for TRIP_IDs {batch_start} to {batch_end}")
            pathQuery = """
            INSERT IGNORE INTO path (tripId, pointId, idx)
            SELECT
            r.ID AS tripId,
            p.pointId,
            j.idx - 1 AS idx
            FROM porto_raw r
            JOIN JSON_TABLE(
            r.POLYLINE, '$[*]'
            COLUMNS (
                idx FOR ORDINALITY,
                lon DECIMAL(10, 7) PATH '$[0]',
                lat DECIMAL(10, 7) PATH '$[1]'
            )
            ) AS j
            JOIN point p ON p.latitude = j.lat AND p.longitude = j.lon
            WHERE JSON_LENGTH(r.POLYLINE) > 0
            AND r.ID BETWEEN %s AND %s
            """
            self.cursor.execute(pathQuery, (batch_start, batch_end))
            self.db_connection.commit()
            print(f"✅ Finished batch {batch_start} to {batch_end}")
        print("Inserted data into path table.")
        print(f"⏱️ Path table insert took {time.time() - start:.2f} seconds.")
        print("✅ Normalization complete (trip, origin_call, origin_stand).")

    def clean_database(self):
        tables = ['path', 'point', 'origin_call', 'origin_stand', 'trip', 'porto_raw']
        for table in tables:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table};")
        self.db_connection.commit()
        print("All tables have been cleaned.")

# ----------------------------------------------------------------------
# Run script
# ----------------------------------------------------------------------
if __name__ == "__main__":
    program = CreateTables()
    # Create the tables
    program.clean_database()
    program.create_trip_table()
    program.create_point_table()
    program.create_path_table()
    program.create_origin_call_table()
    program.create_origin_stand_table()
    
    # Show the tables
    program.show_tables()

    # Load the Porto dataset
    program.load_raw_porto_data('porto/porto/porto.csv')

    # Normalize data into final tables
    program.normalize_raw_data()
    program.connection.close_connection()
