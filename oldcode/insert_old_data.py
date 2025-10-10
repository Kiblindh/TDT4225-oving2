

"""
OLD insert_data, first method looked at, too inefficient. Only kept to show work
If later used, paste it into the createtables.py file in the class and adapt it to the 
current table design. 
"""
    # def insert_old_data(self):
    #     df = self.read_porto_csv(nrows=100) # Read only 100 first for testing
    #     for _, row in df.iterrows():
    #         tripId = int(row['TRIP_ID'])
    #         taxiId = int(row['TAXI_ID'])
    #         startTime = pd.to_datetime(row['TIMESTAMP'], unit='s')
    #         dayType = str(row['DAY_TYPE'])
    #         missingData = bool(row['MISSING_DATA'])

    #         tripQuery = """
    #         INSERT INTO trip (tripId, taxiId, startTime, dayType, missingData) VALUES (%s, %s, %s, %s, %s)
    #         """
    #         self.cursor.execute(tripQuery, (tripId, taxiId, startTime, dayType, missingData))
            
    #         callerId = int(row['ORIGIN_CALL']) if not pd.isnull(row['ORIGIN_CALL']) else None
    #         if(callerId is not None):
    #             originCallQuery = """
    #             INSERT INTO origin_call (tripId, callerId) VALUES (%s, %s)"""
    #             self.cursor.execute(originCallQuery, (tripId, callerId))

    #         standId = int(row['ORIGIN_STAND']) if not pd.isnull(row['ORIGIN_STAND']) else None
    #         if(standId is not None):
    #             originStandQuery = """
    #             INSERT INTO origin_stand (tripId, standId) VALUES (%s, %s)"""
    #             self.cursor.execute(originStandQuery, (tripId, standId))

    #         polyline = ast.literal_eval(row['POLYLINE'])
    #         for idx, point in enumerate(polyline):
    #             longitude = float(point[0])
    #             latitude = float(point[1])
    #             try: 
    #                 pointQuery = """
    #                 INSERT INTO point (latitude, longitude) VALUES (%s, %s)
    #                 """
    #                 self.cursor.execute(pointQuery, (latitude, longitude))
    #                 pointId = self.cursor.lastrowid # Get the auto-incremented pointId (primary key of last execution)
    #             except mysql.connector.errors.IntegrityError as e:
    #                 if e.errno == 1062: # Duplicate entry
    #                     self.cursor.execute("SELECT pointId FROM point WHERE latitude = %s AND longitude = %s", (latitude, longitude))
    #                     pointId = self.cursor.fetchone()[0]
    #                 else:
    #                     raise
    #             if pointId is not None:
    #                 pathQuery = """
    #                 INSERT INTO path (tripId, pointId, idx) VALUES (%s, %s, %s)
    #                 """
    #                 self.cursor.execute(pathQuery, (tripId, pointId, idx))
    #     self.db_connection.commit()
