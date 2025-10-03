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
                   COUNT(DISTINCT taxiId) AS Taxis
            FROM trip;
        """
        queryPoints = """
            SELECT COUNT(*) AS Points
            FROM point;
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
                    COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT taxiId), 0), 2
                ) AS AverageTripsPerTaxi
            FROM trip;
        """
        self.cursor.execute(query)
        AverageTripsPerTaxi = self.cursor.fetchone()
        return AverageTripsPerTaxi

    #List the top 20 taxis with the most trips.
    def task3(self): 
        query = """
            SELECT
                taxiId,
                COUNT(*) AS Trips
            FROM trip
            GROUP BY taxiId
            ORDER BY Trips DESC
            LIMIT 20;
        """
        self.cursor.execute(query)
        Top20Taxis = self.cursor.fetchall()
        return Top20Taxis


    # What is the most used call type per taxi?
    def task4a(self): #TODO Write command
        query = """
            WITH TripCallTypes AS (
                SELECT 
                    tripTable.tripId,
                    tripTable.taxiId,
                    CASE
                        WHEN originCall.callerId IS NOT NULL THEN 'A'
                        WHEN originStand.standId IS NOT NULL THEN 'B'
                        ELSE 'C'
                    END AS callType
                FROM trip tripTable
                LEFT JOIN origin_call originCall ON tripTable.tripId = originCall.tripId
                LEFT JOIN origin_stand originStand ON tripTable.tripId = originStand.tripId
            ),
            Counts AS (
                SELECT
                    taxiId,
                    callType,
                    COUNT(*) AS callCount,
                    ROW_NUMBER() OVER (PARTITION BY taxiId ORDER BY COUNT(*) DESC) AS rn
                FROM TripCallTypes
                GROUP BY taxiId, callType
            )
            SELECT taxiId, callType, callCount
            FROM Counts
            WHERE rn = 1;
            """

        self.cursor.execute(query)
        results = self.cursor.fetchall()
        print(tabulate(results, headers=[desc[0] for desc in self.cursor.description]))
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
                    p.tripId, 
                    p.idx, 
                    pt.latitude AS lat, 
                    pt.longitude AS lon
                FROM path p
                JOIN point pt ON pt.pointId = p.pointId
            ),
            segments AS (
                SELECT 
                    tripId, 
                    lat, 
                    lon,
                    LEAD(lat) OVER (PARTITION BY tripId ORDER BY idx) AS lat2,
                    LEAD(lon) OVER (PARTITION BY tripId ORDER BY idx) AS lon2
                FROM pts
            ),
            PerTrip AS (
                SELECT 
                    tripId,
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
                GROUP BY tripId
            ),
            TripTaxi AS (
                SELECT 
                    t.taxiId,
                    p.tripId,
                    (GREATEST(p.TotalPoints - 1, 0) * 15) AS Duration,
                    p.Distance
                FROM trip t
                JOIN PerTrip p ON p.tripId = t.tripId
            )
            SELECT 
                taxiId,
                SUM(Duration) / 3600.0 AS TotalHours,
                SUM(Distance) AS TotalDistance
            FROM TripTaxi
            GROUP BY taxiId
            ORDER BY TotalHours DESC, TotalDistance DESC;
        """
        self.cursor.execute(query)
        TaxiHoursDistance = self.cursor.fetchall()
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
                  p.tripId,
                  COUNT(*) AS TotalPoints
                FROM path p
                GROUP BY p.tripId
            )
            SELECT
                COUNT(*) AS InvalidTrips
            FROM trip t
            LEFT JOIN PerTrip pt ON pt.tripId = t.tripId
            WHERE COALESCE(pt.TotalPoints, 0) < 3;
        """
        self.cursor.execute(query)
        InvalidTrips = self.cursor.fetchone()
        return InvalidTrips

    
    #Find pairs of different taxis that were within 5m and within 5 seconds of each
    #other at least once.
    def task8(self):
        query = """
            WITH TripBoundaries AS (
                SELECT 
                    t.tripId,
                    t.taxiId,
                    FROM_UNIXTIME(CAST(t.startTime AS UNSIGNED)) AS TimeStart,
                    FROM_UNIXTIME(CAST(t.startTime AS UNSIGNED)) + INTERVAL (MAX(p.idx) * 15) SECOND AS TimeEnd
                FROM trip t
                JOIN path p ON p.tripId = t.tripId
                GROUP BY t.tripId, t.taxiId, t.startTime
            ),
            CandidateTrips AS (
                SELECT 
                    a.tripId AS TripA, 
                    b.tripId AS TripB,
                    a.taxiId AS TaxiA, 
                    b.taxiId AS TaxiB
                FROM TripBoundaries a
                JOIN TripBoundaries b
                  ON a.taxiId < b.taxiId
                 AND a.TimeStart <= b.TimeEnd + INTERVAL 5 SECOND
                 AND b.TimeStart <= a.TimeEnd + INTERVAL 5 SECOND
            )
            SELECT DISTINCT
                LEAST(e1.taxiId, e2.taxiId)    AS TaxiA,                      
                GREATEST(e1.taxiId, e2.taxiId) AS TaxiB
            FROM CandidateTrips c

            JOIN path pa1 ON pa1.tripId = c.TripA
            JOIN point pt1 ON pt1.pointId = pa1.pointId
            JOIN trip t1   ON t1.tripId = c.TripA

            JOIN path pa2 ON pa2.tripId = c.TripB
            JOIN point pt2 ON pt2.pointId = pa2.pointId
            JOIN trip t2   ON t2.tripId = c.TripB

            CROSS JOIN LATERAL (
                SELECT 
                    FROM_UNIXTIME(CAST(t1.startTime AS UNSIGNED)) + INTERVAL (pa1.idx*15) SECOND AS ts,
                    pt1.longitude AS lon,
                    pt1.latitude  AS lat,
                    t1.taxiId     AS taxiId
            ) AS e1

            CROSS JOIN LATERAL (
                SELECT 
                    FROM_UNIXTIME(CAST(t2.startTime AS UNSIGNED)) + INTERVAL (pa2.idx*15) SECOND AS ts,
                    pt2.longitude AS lon,
                    pt2.latitude  AS lat,
                    t2.taxiId     AS taxiId
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
        return TaxiPairs


    #Find the trips that started on one calendar day and ended on the next (midnight
    #crossers).
    def task9(self):
        query = """
            WITH PerTrip AS (
                SELECT
                    t.tripId,
                    t.startTime AS StartTime,
                    (GREATEST(COUNT(*) - 1, 0) * 15) AS Duration
                FROM trip t
                JOIN path p ON p.tripId = t.tripId
                GROUP BY t.tripId, t.startTime
            ),
            EndCompute AS (
                SELECT
                    tripId,
                    StartTime,
                    StartTime + INTERVAL Duration SECOND AS EndTime
                FROM PerTrip
            )
            SELECT tripId
            FROM EndCompute
            WHERE DATE(StartTime) <> DATE(EndTime)
            ORDER BY tripId;
        """
        self.cursor.execute(query)
        TripIDs = self.cursor.fetchall()
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
        -- First, get start and end times for each trip
        WITH trip_times AS (
            SELECT 
                tripId,
                taxiId,
                startTime,
                startTime + INTERVAL ((
                    SELECT MAX(idx) 
                    FROM path 
                    WHERE path.tripId = trip.tripId
                ) * 15) SECOND AS endTime
            FROM trip
        ),
        -- Next, compute idle times between consecutive trips for each taxi
        idle_times AS (
            SELECT
                taxiId,
                endTime,
                LEAD(startTime) OVER (PARTITION BY taxiId ORDER BY startTime) AS nextStartTime
            FROM trip_times
        )
        -- Finally, calculate average idle time per taxi and return top 20
        SELECT
            taxiId,
            ROUND(AVG(TIMESTAMPDIFF(SECOND, endTime, nextStartTime))/60, 2) AS avgIdleMinutes,
            COUNT(*) AS idlePeriods
        FROM idle_times
        WHERE nextStartTime IS NOT NULL -- Removes taxis with only 1 trip
        GROUP BY taxiId
        ORDER BY avgIdleMinutes DESC
        LIMIT 20;
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        print(tabulate(results, headers=[desc[0] for desc in self.cursor.description]))
        self.db_connection.commit()

#Main function to run all tasks, temporarily here for easy access
def main():
    program = None
    try:
        program = Queries()
        #print(program.task1())
        #print(program.task2())
        #print(program.task3())
        #program.task4a()
        #program.task4b()
        #print(program.task5())
        #program.task6()
        #print(program.task7())
        #print(program.task8())
        print(program.task9())
        #program.task10()
        #program.task11()
    except Exception as e:
        print("ERROR: Failed to run queries:", e)
    finally:
        if program is not None:
            program.connection.close_connection()

if __name__ == "__main__":
    main()