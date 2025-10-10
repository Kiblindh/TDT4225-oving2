
import mysql
from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
from datetime import datetime
import ast


class CreateTables:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_trip_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                tripId INT AUTO_INCREMENT PRIMARY KEY,
                originalTripId BIGINT NOT NULL,
                taxiId BIGINT NOT NULL,
                startTime DATETIME NOT NULL,
                dayType CHAR(1) NOT NULL,
                missingData BOOLEAN NOT NULL
                )
        """
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_point_table(self, table_name):
        query = '''
            CREATE TABLE IF NOT EXISTS %s (
                pointId INT PRIMARY KEY AUTO_INCREMENT,
                latitude DOUBLE NOT NULL,
                longitude DOUBLE NOT NULL,
                UNIQUE (latitude, longitude)
            )
        '''
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_path_table(self, table_name):
        query = '''
            CREATE TABLE IF NOT EXISTS %s(
                tripId INT NOT NULL,
                pointId INT NOT NULL,
                idx INT NOT NULL,
                FOREIGN KEY (tripId) REFERENCES trip(tripId) ON DELETE CASCADE,
                FOREIGN KEY (pointId) REFERENCES point(pointId) ON DELETE CASCADE,
                PRIMARY KEY (tripId, idx)
            )
        '''
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_origin_call_table(self, table_name):
        query = '''
            CREATE TABLE IF NOT EXISTS %s(
                tripId INT PRIMARY KEY,
                callerId INT NOT NULL,
                FOREIGN KEY (tripId) REFERENCES trip(tripId)
            )
        '''
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_origin_stand_table(self, table_name):
        query = '''
            CREATE TABLE IF NOT EXISTS %s(
                tripId INT PRIMARY KEY,
                standId INT NOT NULL,
                FOREIGN KEY (tripId) REFERENCES trip(tripId)
            )
        '''
        self.cursor.execute(query % table_name)
        self.db_connection.commit()
    
    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def read_porto_csv(self, filepath='porto.csv', **kwargs):
        """
        Reads the porto.csv file and returns a pandas DataFrame.
        """
        df = pd.read_csv(filepath, **kwargs)
        return df
    
    def clean_database(self):
        tables = ['path', 'point', 'origin_call', 'origin_stand', 'trip', 'porto_raw']
        for table in tables:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table};")
        self.db_connection.commit()
        print("All tables have been cleaned.")

    def insert_data(self, chunksize=10000):
        df_iter = self.read_porto_csv(chunksize=chunksize)  # specify nrows for testing faster
        index_tripId = 1
        chunk_count = 0
        for df in df_iter:
            trips = [] # tripId, originalTripId, taxiId, startTime, dayType, missingData
            origin_calls = []
            origin_stands = []
            tmp_paths = []  # (tripId, idx, latitude, longitude) - pointId comes later through staging table

            # Create lists used for bulk insertion
            for row in df.itertuples(index=False):
                originalTripId = int(getattr(row, 'TRIP_ID'))
                tripId = index_tripId
                index_tripId += 1
                taxiId = int(getattr(row, 'TAXI_ID'))
                startTime = datetime.fromtimestamp(int(getattr(row, 'TIMESTAMP')))
                dayType = str(getattr(row, 'DAY_TYPE'))
                missingData = bool(getattr(row, 'MISSING_DATA'))

                trips.append((tripId, originalTripId,taxiId, startTime, dayType, missingData))

                callerId = getattr(row, 'ORIGIN_CALL')
                if pd.notnull(callerId):
                    origin_calls.append((tripId, int(callerId)))

                standId = getattr(row, 'ORIGIN_STAND')
                if pd.notnull(standId):
                    origin_stands.append((tripId, int(standId)))

                poly = ast.literal_eval(getattr(row, 'POLYLINE') or '[]')
                for idx, (lon, lat) in enumerate(poly):
                    tmp_paths.append((tripId, idx, float(lat), float(lon)))  # store lat, lon in this order for the UNIQUE index

            cursor = self.cursor
            connection = self.db_connection
            try:
                # Bulk trip insertion
                cursor.executemany(
                    """
                    INSERT IGNORE INTO trip (tripId, originalTripId, taxiId, startTime, dayType, missingData)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    trips
                )
                connection.commit()
                # Bulk origin_call insertion
                if origin_calls:
                    cursor.executemany(
                        "INSERT INTO origin_call (tripId, callerId) VALUES (%s, %s)",
                        origin_calls
                    )
                    connection.commit()
                # Bulk origin_stand insertion
                if origin_stands:
                    cursor.executemany(
                        "INSERT INTO origin_stand (tripId, standId) VALUES (%s, %s)",
                        origin_stands
                    )
                    connection.commit()

                # Temp path table to ensure bulk path insertion 
                cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_paths")
                cursor.execute("""
                    CREATE TEMPORARY TABLE tmp_paths (
                        tripId   BIGINT NOT NULL,
                        idx      INT NOT NULL,
                        latitude DOUBLE NOT NULL,
                        longitude DOUBLE NOT NULL,
                        KEY (latitude, longitude),
                        PRIMARY KEY (tripId, idx) -- For fast lookups
                    ) ENGINE=InnoDB
                """)
                connection.commit()

                # Bulk load staged points
                if tmp_paths:
                    cursor.executemany(
                        "INSERT IGNORE INTO tmp_paths (tripId, idx, latitude, longitude) VALUES (%s, %s, %s, %s)",
                        tmp_paths
                    )
                    connection.commit()

                    # Create points by using all unique lat, lon from paths in staged table
                    cursor.execute("""
                        INSERT IGNORE INTO point (latitude, longitude)
                        SELECT DISTINCT tmpPath.latitude, tmpPath.longitude
                        FROM tmp_paths tmpPath
                    """)

                    # Join staged paths with points to get pointId, then insert all paths into main path table
                    cursor.execute("""
                        INSERT IGNORE INTO path (tripId, pointId, idx)
                        SELECT path.tripId, currentPoint.pointId, path.idx
                        FROM tmp_paths path
                        JOIN point currentPoint
                        ON currentPoint.latitude = path.latitude AND currentPoint.longitude = path.longitude
                    """)
                connection.commit()
                chunk_count += 1
                print(f"Finished chunk {chunk_count} with {chunksize} trips.")
            except:
                connection.rollback()
                raise


def main():
    program = None
    
    try:
        program = CreateTables()
        # program.clean_database()
        program.create_trip_table("trip")
        program.create_point_table("point")
        program.create_path_table("path")
        program.create_origin_call_table("origin_call")
        program.create_origin_stand_table("origin_stand")
        program.show_tables()
        # program.insert_data()

    except Exception as e:
        print("ERROR: Failed to run example:", e)
    finally:
        if program is not None:
            program.connection.close_connection()


if __name__ == "__main__":
    main()