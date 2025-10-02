
import mysql
from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import ast


class CreateTables:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_trip_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                tripId BIGINT PRIMARY KEY,
                taxiId VARCHAR(50) NOT NULL,
                startTime TEXT NOT NULL,
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
                tripId BIGINT NOT NULL,
                pointId INTEGER NOT NULL,
                idx INTEGER NOT NULL,
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
                tripId BIGINT PRIMARY KEY,
                callerId INTEGER NOT NULL,
                FOREIGN KEY (tripId) REFERENCES trip(tripId)
            )
        '''
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_origin_stand_table(self, table_name):
        query = '''
            CREATE TABLE IF NOT EXISTS %s(
                tripId BIGINT PRIMARY KEY,
                standId INTEGER NOT NULL,
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

    def read_porto_csv(self, filepath='porto/porto/porto.csv', **kwargs):
        """
        Reads the porto.csv file and returns a pandas DataFrame.
        """
        df = pd.read_csv(filepath, **kwargs)
        return df

    def insert_data(self):
        df = self.read_porto_csv(nrows=100) # Read only 100 first for testing
        for _, row in df.iterrows():
            tripId = int(row['TRIP_ID'])
            taxiId = int(row['TAXI_ID'])
            startTime = pd.to_datetime(row['TIMESTAMP'], unit='s')
            dayType = str(row['DAY_TYPE'])
            missingData = bool(row['MISSING_DATA'])

            tripQuery = """
            INSERT INTO trip (tripId, taxiId, startTime, dayType, missingData) VALUES (%s, %s, %s, %s, %s)
            """
            self.cursor.execute(tripQuery, (tripId, taxiId, startTime, dayType, missingData))
            
            callerId = int(row['ORIGIN_CALL']) if not pd.isnull(row['ORIGIN_CALL']) else None
            if(callerId is not None):
                originCallQuery = """
                INSERT INTO origin_call (tripId, callerId) VALUES (%s, %s)"""
                self.cursor.execute(originCallQuery, (tripId, callerId))

            standId = int(row['ORIGIN_STAND']) if not pd.isnull(row['ORIGIN_STAND']) else None
            if(standId is not None):
                originStandQuery = """
                INSERT INTO origin_stand (tripId, standId) VALUES (%s, %s)"""
                self.cursor.execute(originStandQuery, (tripId, standId))

            polyline = ast.literal_eval(row['POLYLINE'])
            for idx, point in enumerate(polyline):
                longitude = float(point[0])
                latitude = float(point[1])
                try: 
                    pointQuery = """
                    INSERT INTO point (longitude, latitude) VALUES (%s, %s)
                    """
                    self.cursor.execute(pointQuery, (longitude, latitude))
                    pointId = self.cursor.lastrowid # Get the auto-incremented pointId (primary key of last execution)
                except mysql.connector.errors.IntegrityError as e:
                    if e.errno == 1062: # Duplicate entry
                        self.cursor.execute("SELECT pointId FROM point WHERE latitude = %s AND longitude = %s", (latitude, longitude))
                        pointId = self.cursor.fetchone()[0]
                    else:
                        raise
                if pointId is not None:
                    pathQuery = """
                    INSERT INTO path (tripId, pointId, idx) VALUES (%s, %s, %s)
                    """
                    self.cursor.execute(pathQuery, (tripId, pointId, idx))
        self.db_connection.commit()

    def clean_database(self):
        tables = ['path', 'point', 'origin_call', 'origin_stand', 'trip']
        for table in tables:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table};")
        self.db_connection.commit()
        print("All tables have been cleaned.")

def main():
    program = None
    try:
        program = CreateTables()
        program.clean_database()
        program.create_trip_table("trip")
        program.create_point_table("point")
        program.create_path_table("path")
        program.create_origin_call_table("origin_call")
        program.create_origin_stand_table("origin_stand")
        program.show_tables()
        program.insert_data()
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to run example:", e)
    finally:
        if program is not None:
            program.connection.close_connection()


if __name__ == "__main__":
    main()