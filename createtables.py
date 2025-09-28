
from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd

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
                pointId INT PRIMARY KEY,
                latitude DOUBLE NOT NULL,
                longitude DOUBLE NOT NULL
            )
        '''
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_path_table(self, table_name):
        query = '''
            CREATE TABLE IF NOT EXISTS %s(
                tripId INTEGER NOT NULL,
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
                tripId INT PRIMARY KEY,
                callerId INTEGER NOT NULL,
                FOREIGN KEY (tripId) REFERENCES trip(tripId)
            )
        '''
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_origin_stand_table(self, table_name):
        query = '''
            CREATE TABLE IF NOT EXISTS %s(
                tripId INT PRIMARY KEY,
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

    def read_porto_csv(filepath='porto/porto/porto.csv'):
        """
        Reads the porto.csv file and returns a pandas DataFrame.
        """
        df = pd.read_csv(filepath)
        return df

    def insert_data(self):
        df = self.read_porto_csv()
        for row in df:
            tripId = row['TRIP_ID'].astype(int)
            taxiId = row['TAXI_ID'].astype(int)
            startTime = pd.to_datetime(row['TIMESTAMP'], unit='s')
            dayType = row['DAY_TYPE'].astype(str)
            missingData = row['MISSING_DATA'].astype(bool)
            callerId = row['ORIGIN_CALL'].astype(int) if not pd.isnull(row['ORIGIN_CALL']) else None
            standId = row['ORIGIN_STAND'].astype(int) if not pd.isnull(row['ORIGIN_STAND']) else None
            

            tripQuery = """
            INSERT INTO trip (tripId, taxiId, startTime, dayType, missingData) VALUES (%s, %s, %s, %s, %s)
            """

            self.cursor.execute(tripQuery, (tripId, taxiId, startTime, dayType, missingData))
        self.db_connection.commit()

def main():
    program = None
    try:
        program = CreateTables()
        program.create_trip_table("trip")
        program.create_point_table("point")
        program.create_path_table("path")
        program.create_origin_call_table("origin_call")
        program.create_origin_stand_table("origin_stand")
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to run example:", e)
    finally:
        if program is not None:
            program.connection.close_connection()


if __name__ == "__main__":
    main()