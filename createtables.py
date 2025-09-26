
from DbConnector import DbConnector
from tabulate import tabulate

class CreateTables:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_trip_table(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                tripId INT PRIMARY KEY,
                taxiId VARCHAR(50) NOT NULL,
                startTime TEXT NOT NULL)
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
                FOREIGN KEY (tripId) REFERENCES trip(tripId),
                FOREIGN KEY (pointId) REFERENCES point(pointId),
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