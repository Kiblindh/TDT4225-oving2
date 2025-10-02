from DbConnector import DbConnector
from tabulate import tabulate

class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


    # How many taxis, trips, and total GPS points are there?
    def task1(self):
        queryTrips = """
            SELECT COUNT(*) AS trips,
                   COUNT(DISTINCT TAXI_ID) AS Taxis
            FROM Trip;
        """
        queryPoints = """
            SELECT COUNT(*) AS Points
            FROM Point;
        """
        self.cursor.execute(queryTrips)
        TotalTrips = self.cursor.fetchone()
        self.cursor.execute(queryPoints)
        TotalPoints = self.cursor.fetchone()
        self.db_connection.commit()
        return TotalTrips, TotalPoints
    
    # What is the average number of trips per taxi?
    def task2(self):
        query = """
            SELECT
                ROUND(
                    COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT TAXI_ID), 0), 2
                ) AS AverageTripsPerTaxi
            FROM Trip;
        """
        self.cursor.execute(query)
        AverageTripsPerTaxi = self.cursor.fetchone()
        self.db_connection.commit()
        return AverageTripsPerTaxi

    #List the top 20 taxis with the most trips.
    def task3(self): 
        query = """
            SELECT
            TAXI_ID,
            COUNT(*) AS Trips
            FROM Trip
            GROUP BY TAXI_ID
            ORDER BY Trips DESC
            LIMIT 20;
        """
        self.cursor.execute(query)
        Top20Taxis = self.cursor.fetchall()
        self.db_connection.commit()
        return Top20Taxis

    # What is the most used call type per taxi?
    def task4a(self): #TODO Write command
        query = """

            """

        self.cursor.execute(query)
        self.db_connection.commit()

    # For each call type, compute the average trip duration and distance, and also
    # #report the share of trips starting in four time bands: 00–06, 06–12, 12–18, and
    # 18–24.
    def task4b(self): #TODO Write command
        query = """

            """

        self.cursor.execute(query)
        self.db_connection.commit()

    #Find the taxis with the most total hours driven as well as total distance driven.
    #List them in order of total hours.
    def task5(self):
        query = """
            WITH pts AS (
                SELECT 
                    p.TripID, 
                    p.index, 
                    pt.Latitude AS lat, 
                    pt.Longitude AS lon
                FROM Path p
                JOIN Point pt ON pt.PointID = p.PointID
            ),
            segments AS (
                SELECT 
                    TripID, 
                    lat, 
                    lon,
                    LEAD(lat) OVER (PARTITION BY TripID ORDER BY index) AS lat2,
                    LEAD(lon) OVER (PARTITION BY TripID ORDER BY index) AS lon2
                FROM pts
            ),
            PerTrip AS (
                SELECT 
                    TripID,
                    COUNT(*) + 1 AS TotalPoints,
                    SUM(
                        2 * 6371000 * ASIN(
                            SQRT(
                                POWER(SIN(RADIANS((lat2 - lat) / 2)), 2) +
                                COS(RADIANS(lat)) * COS(RADIANS(lat2)) *
                                POWER(SIN(RADIANS((lon2 - lon) / 2)), 2)
                            )
                        )
                    ) AS Distance
                FROM segments
                WHERE lat2 IS NOT NULL
                GROUP BY TripID
            ),
            TripTaxi AS (
                SELECT 
                    t.TAXI_ID,
                    p.TripID,
                    (GREATEST(p.TotalPoints - 1, 0) * 15) AS Duration,
                    p.Distance
                FROM Trip t
                JOIN PerTrip p ON p.TripID = t.Trip_ID
            )
            SELECT 
                TAXI_ID,
                SUM(Duration) / 3600.0 AS TotalHours,
                SUM(Distance) AS TotalDistance           
            FROM TripTaxi
            GROUP BY TAXI_ID
            ORDER BY TotalHours DESC, TotalDistance DESC;

        """
        self.cursor.execute(query)
        TaxiHoursDistance = self.cursor.fetchall()
        self.db_connection.commit()
        return TaxiHoursDistance

    # Find the trips that passed within 100 m of Porto City Hall.
    #(longitude, latitude) = (-8.62911, 41.15794)
    def task6(self): #TODO Write command
        query = """
            
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    #Identify the number of invalid trips. An invalid trip is defined as a trip with fewer
    #than 3 GPS points
    def task7(self):
        query = """
            WITH PerTrip AS (
                SELECT
                  p.TripID,
                  COUNT(*) AS TotalPoints
                FROM Path p
                GROUP BY p.TripID
            )
            SELECT
                COUNT(*) AS InvalidTrips
            FROM Trip t
            LEFT JOIN PerTrip pt ON pt.TripID = t.Trip_ID
            WHERE COALESCE(pt.TotalPoints, 0) < 3;
        """
        self.cursor.execute(query)
        InvalidTrips = self.cursor.fetchone()
        self.db_connection.commit()
        return InvalidTrips
    
    #Find pairs of different taxis that were within 5m and within 5 seconds of each
    #other at least once.
    def task8(self):
        query = """
            WITH TripBoundaries AS (
                SELECT 
                    t.Trip_ID,
                    t.TAXI_ID,
                    FROM_UNIXTIME(t.TIMESTAMP) AS TimeStart,
                    FROM_UNIXTIME(t.TIMESTAMP) + INTERVAL (MAX(p.`index`) * 15) SECOND AS TimeEnd
                FROM Trip t
                JOIN Path p ON p.TripID = t.Trip_ID
                GROUP BY t.Trip_ID, t.TAXI_ID, t.TIMESTAMP
            ),

            candidate_trips AS (
                SELECT 
                    a.Trip_ID AS TripA, 
                    b.Trip_ID AS TripB,
                    a.TAXI_ID AS TaxiA, 
                    b.TAXI_ID AS TaxiB
                FROM TripBoundaries a

                JOIN TripBoundaries b
                  ON a.TAXI_ID < b.TAXI_ID
                 AND a.TimeStart <= b.TimeEnd + INTERVAL 5 SECOND
                 AND b.TimeStart <= a.TimeEnd + INTERVAL 5 SECOND
            )
            SELECT DISTINCT
                LEAST(e1.TAXI_ID, e2.TAXI_ID)    AS TaxiA,                      
                GREATEST(e1.TAXI_ID, e2.TAXI_ID) AS TaxiB
            FROM candidate_trips c

            JOIN Path pa1 ON pa1.TripID = c.TripA
            JOIN Point pt1 ON pt1.PointID = pa1.PointID
            JOIN Trip t1   ON t1.Trip_ID = c.TripA

            JOIN Path pa2 ON pa2.TripID = c.TripB
            JOIN Point pt2 ON pt2.PointID = pa2.PointID
            JOIN Trip t2   ON t2.Trip_ID = c.TripB
            CROSS JOIN LATERAL (
                SELECT 
                    FROM_UNIXTIME(t1.TIMESTAMP) + INTERVAL (pa1.index*15) SECOND AS ts,
                    pt1.Longitude AS lon,
                    pt1.Latitude  AS lat,
                    t1.TAXI_ID    AS TAXI_ID
            ) AS e1

            CROSS JOIN LATERAL (
                SELECT 
                    FROM_UNIXTIME(t2.TIMESTAMP) + INTERVAL (pa2.index*15) SECOND AS ts,
                    pt2.Longitude AS lon,
                    pt2.Latitude  AS lat,
                    t2.TAXI_ID    AS TAXI_ID
            ) AS e2
            
            WHERE ABS(TIMESTAMPDIFF(SECOND, e1.ts, e2.ts)) <= 5
              AND ABS(e1.lat - e2.lat) < 0.00006
              AND ABS(e1.lon - e2.lon) < 0.00006 / COS(RADIANS((e1.lat + e2.lat)/2))
              AND (
                 2*6371000*ASIN(
                   SQRT(
                     POWER(SIN(RADIANS((e2.lat - e1.lat)/2)),2) +
                     COS(RADIANS(e1.lat))*COS(RADIANS(e2.lat))*
                     POWER(SIN(RADIANS((e2.lon - e1.lon)/2)),2)
                   )
                 )
              ) <= 5;         
        """
        self.cursor.execute(query)
        TaxiPairs = self.cursor.fetchall()
        self.db_connection.commit()
        return TaxiPairs

    #Find the trips that started on one calendar day and ended on the next (midnight
    #crossers).
    def task9(self):
        query = """
            WITH PerTrip AS (
                SELECT
                    Trip.Trip_ID,
                    FROM_UNIXTIME(Trip.timestamp) AS StartTime,
                    (GREATEST(COUNT(*) - 1, 0) * 15) AS Duration
                FROM Trip
                JOIN Path ON Path.TripID = Trip.Trip_ID
                GROUP BY Trip.Trip_ID, Trip.timestamp
            ),
            EndCompute AS (
                SELECT
                    Trip_ID,
                    StartTime,
                    StartTime + INTERVAL Duration SECOND AS EndTime
                FROM PerTrip
            )
            SELECT Trip_ID
            FROM EndCompute
            WHERE DATE(StartTime) <> DATE(EndTime)
            ORDER BY Trip_ID;
        """
        self.cursor.execute(query)
        TripIDs = self.cursor.fetchall()
        self.db_connection.commit()
        return TripIDs
    
    #Find the trips whose start and end points are within 50 m of each other (circular
    #trips).
    def task10(self): #TODO Write command
        query = """
            WITH min_max_path AS (
            SELECT
                shortened_path.tripId,
                min_path.pointId AS first_pointId,
                max_path.pointId AS last_pointId
            FROM (
                SELECT tripId, MIN(idx) AS min_idx, MAX(idx) AS max_idx
                FROM path
                GROUP BY tripId
            ) AS shortened_path
            JOIN path AS min_path
                ON min_path.tripId = shortened_path.tripId AND min_path.idx = shortened_path.min_idx
            JOIN path AS max_path
                ON max_path.tripId = shortened_path.tripId AND max_path.idx = shortened_path.max_idx
            ),
            -- join those to the point table
            trips AS (
                SELECT
                    trip.tripId,
                    p_first.latitude AS first_lat,
                    p_first.longitude AS first_lon,
                    p_last.latitude AS last_lat,
                    p_last.longitude AS last_lon
                FROM min_max_path trip
                JOIN point p_first ON p_first.pointId = trip.first_pointId
                JOIN point p_last ON p_last.pointId  = trip.last_pointId
                )
            SELECT
                tripId,
                first_lat,
                first_lon,
                last_lat,
                last_lon,
                2 * 6371000 * ASIN(
                    SQRT(
                        POWER(SIN(RADIANS((last_lat - first_lat) / 2)), 2) +
                        COS(RADIANS(first_lat)) * COS(RADIANS(last_lat)) *
                        POWER(SIN(RADIANS((last_lon - first_lon) / 2)), 2)
                    )
                ) AS travelDistanceInMeters
            FROM trips
            HAVING travelDistanceInMeters < 50
            ORDER BY tripId;
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        print(tabulate(results, headers=[desc[0] for desc in self.cursor.description]))
        self.db_connection.commit()

    #For each taxi, compute the average idle time between consecutive trips. List the
    #top 20 taxis with the highest average idle time.
    def task11(self): #TODO Write command
        query = """
            
        """
        self.cursor.execute(query)
        self.db_connection.commit()

#Main function to run all tasks, temporarily here for easy access
def main():
    program = None
    try:
        program = Queries()
        #program.task1()
        #program.task2()
        #program.task3()
        #program.task4a()
        #program.task4b()
        #program.task5()
        #program.task6()
        #program.task7()
        #program.task8()
        #program.task9()
        program.task10()
        #program.task11()
    except Exception as e:
        print("ERROR: Failed to run queries:", e)
    finally:
        if program is not None:
            program.connection.close_connection()

if __name__ == "__main__":
    main()