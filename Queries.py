from unittest import result
from DbConnector import DbConnector
from tabulate import tabulate
from datetime import timedelta
import pandas as pd
from scipy.spatial import cKDTree
import numpy as np
import os
import warnings

warnings.filterwarnings("ignore", category=UserWarning) #Added this to ignore pandas warnings during task 8

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
        trips_data = self.cursor.fetchone()
        self.cursor.execute(queryPoints)
        points_data = self.cursor.fetchone()

        output = (
            f"Total trips:   {trips_data[0]:,}\n"
            f"Distinct taxis: {trips_data[1]:,}\n"
            f"Total points:   {points_data[0]:,}\n"
        )

        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task1Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to file in results/task1Output.txt")

        self.db_connection.commit()
    
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
        result = self.cursor.fetchone()

        output = f"Average trips per taxi: {result[0]}\n"
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task2Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to results/task2Output.txt")

        self.db_connection.commit()

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
        results = self.cursor.fetchall()

        output = tabulate(results, headers=["Taxi ID", "Trips"], tablefmt="pretty")
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task3Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to results/task3Output.txt")

        self.db_connection.commit()


    # What is the most used call type per taxi?
    def task4a(self):
        query = """
            WITH TripCallTypes AS (
                SELECT 
                    tripTable.tripId,
                    tripTable.taxiId,
                    CASE
                        WHEN origin_call.callerId IS NOT NULL THEN 'A'
                        WHEN origin_stand.standId IS NOT NULL THEN 'B'
                        ELSE 'C'
                    END AS callType
                FROM trip tripTable
                LEFT JOIN origin_call origin_call ON tripTable.tripId = origin_call.tripId
                LEFT JOIN origin_stand origin_stand ON tripTable.tripId = origin_stand.tripId
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
            WHERE rn = 1
            """

        self.cursor.execute(query)
        results = self.cursor.fetchall()

        output = tabulate(results, headers=[desc[0] for desc in self.cursor.description], tablefmt="pretty")
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task4aOutput.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to results/task4aOutput.txt")

        self.db_connection.commit()

    # For each call type, compute the average trip duration and distance, and also
    # #report the share of trips starting in four time bands: 00–06, 06–12, 12–18, and
    # 18–24.
    def task4b(self):
        query = """
         
            WITH tripBoundaries AS (
                SELECT
                    p.tripId,
                    MIN(p.idx) AS firstID,
                    MAX(p.idx) AS lastID
                FROM path AS p
                GROUP BY p.tripId
            ),
            tripPoints AS (
                SELECT
                    tb.tripId,
                    pf.latitude  AS startLat,
                    pf.longitude AS startLon,
                    pl.latitude  AS endLat,
                    pl.longitude AS endLon,
                    (tb.lastID - tb.firstID) * 15 AS Duration
                FROM tripBoundaries AS tb
                JOIN path AS pf_path ON pf_path.tripId = tb.tripId AND pf_path.idx = tb.firstID
                JOIN point AS pf ON pf.pointId = pf_path.pointId
                JOIN path AS pl_path ON pl_path.tripId = tb.tripId AND pl_path.idx = tb.lastID
                JOIN point AS pl ON pl.pointId = pl_path.pointId
            ),
            tripDistance AS (
                SELECT
                    tripId,
                    Duration,
                    2 * 6371000 * ASIN(
                        SQRT(
                            POWER(SIN(RADIANS((endLat - startLat)/2)), 2) +
                            COS(RADIANS(startLat)) * COS(RADIANS(endLat)) *
                            POWER(SIN(RADIANS((endLon - startLon)/2)), 2)
                        )
                    ) AS Distance
                FROM tripPoints
            ),
            tripInfo AS (
                SELECT
                    t.tripId,
                    CASE
                        WHEN oc.tripId IS NOT NULL THEN 'Call'
                        WHEN os.tripId IS NOT NULL THEN 'Stand'
                        ELSE 'Street'
                    END AS callType,
                    t.startTime,
                    d.Duration / 60 AS DurationMinutes,
                    d.Distance / 1000 AS DistanceKilometers
                FROM trip AS t
                LEFT JOIN origin_call  AS oc ON oc.tripId = t.tripId
                LEFT JOIN origin_stand AS os ON os.tripId = t.tripId
                JOIN tripDistance AS d ON d.tripId = t.tripId
            )
            SELECT
                callType,
                ROUND(AVG(DurationMinutes), 2) AS AverageDurationMinutes,
                ROUND(AVG(DistanceKilometers), 2)  AS AverageDistanceKilometers,
                ROUND(SUM(CASE WHEN HOUR(startTime) BETWEEN 0  AND 5  THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS 00_06,
                ROUND(SUM(CASE WHEN HOUR(startTime) BETWEEN 6  AND 11 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS 00_12,
                ROUND(SUM(CASE WHEN HOUR(startTime) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS 00_18,
                ROUND(SUM(CASE WHEN HOUR(startTime) BETWEEN 18 AND 23 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS 00_24
            FROM tripInfo
            GROUP BY callType
            ORDER BY callType;
        """

        self.cursor.execute(query)
        results = self.cursor.fetchall()

        headers = [desc[0] for desc in self.cursor.description]
        output = tabulate(results, headers=headers, tablefmt="pretty")
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task4bOutput.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to results/task4bOutput.txt")

        self.db_connection.commit()

    #Find the taxis with the most total hours driven as well as total distance driven.
    #List them in order of total hours.
    def task5(self):
        query = """
            WITH tripBoundaries AS (
                SELECT 
                    tripId,
                    MIN(idx) AS firstID,
                    MAX(idx) AS lastID
                FROM path
                GROUP BY tripId
            ),
            tripMetrics AS (
                SELECT
                    tb.tripId,
                    (tb.lastID - tb.firstID) * 15 AS DurationSeconds,
                    pStart.latitude  AS startLat,
                    pStart.longitude AS startLon,
                    pEnd.latitude    AS endLat,
                    pEnd.longitude   AS endLon
                FROM tripBoundaries tb
                JOIN path ps ON ps.tripId = tb.tripId AND ps.idx = tb.firstID
                JOIN point pStart ON pStart.pointId = ps.pointId
                JOIN path pe ON pe.tripId = tb.tripId AND pe.idx = tb.lastID
                JOIN point pEnd ON pEnd.pointId = pe.pointId
            ),
            trip_distance AS (
                SELECT
                    tripId,
                    DurationSeconds,
                    2 * 6371000 * ASIN(
                        SQRT(
                            POWER(SIN(RADIANS((endLat - startLat)/2)), 2) +
                            COS(RADIANS(startLat)) * COS(RADIANS(endLat)) *
                            POWER(SIN(RADIANS((endLon - startLon)/2)), 2)
                        )
                    ) AS DistanceMeters
                FROM tripMetrics
            ),
            taxi_totals AS (
                SELECT 
                    t.taxiId,
                    SUM(d.DurationSeconds) / 3600.0 AS TotalHours,
                    SUM(d.DistanceMeters) / 1000.0  AS TotalDistanceKm
                FROM trip t
                JOIN trip_distance d ON d.tripId = t.tripId
                GROUP BY t.taxiId
            )
            SELECT 
                taxiId,
                ROUND(TotalHours, 2) AS TotalHours,
                ROUND(TotalDistanceKm, 2) AS TotalDistanceKm
            FROM taxi_totals
            ORDER BY TotalHours DESC
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        headers = ["Taxi ID", "Total Hours", "Total Kilometers"]
        output = tabulate(results, headers=headers, tablefmt="pretty")
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task5Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to file in results/task5Output.txt")

        self.db_connection.commit()

    # Find the trips that passed within 100 m of Porto City Hall.
    #(longitude, latitude) = (-8.62911, 41.15794)
    def task6(self): 
        query = """
            WITH NearbyPoints AS (
                SELECT 
                    p.TripID,
                    2 * 6371000 * ASIN(
                        SQRT(
                            POWER(SIN(RADIANS((pt.latitude - 41.15794) / 2)), 2) +
                            COS(RADIANS(41.15794)) * COS(RADIANS(pt.latitude)) *
                            POWER(SIN(RADIANS((pt.longitude - (-8.62911)) / 2)), 2)
                        )
                    ) AS DistanceToCityHall
                FROM path AS p
                JOIN point AS pt ON pt.PointID = p.PointID
                WHERE 
                    pt.latitude  BETWEEN 41.15794 - 0.0010 AND 41.15794 + 0.0010
                    AND pt.longitude BETWEEN -8.62911 - 0.0015 AND -8.62911 + 0.0015
            )
            SELECT DISTINCT t.originalTripID
            FROM NearbyPoints np
            JOIN trip t ON np.TripID = t.TripID
            WHERE np.DistanceToCityHall <= 100
            ORDER BY t.originalTripID;
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        output = tabulate(results[:20], headers=["tripId"], tablefmt="pretty")
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task6Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to file in results/task6Output.txt")

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
        result = self.cursor.fetchone()
        result = [(result[0],)]

        headers = [desc[0] for desc in self.cursor.description]
        output = tabulate(result, headers=headers, tablefmt="pretty")
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task7Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to file in results/task7Output.txt")

        self.db_connection.commit()

    #Find pairs of different taxis that were within 5m and within 5 seconds of each
    #other at least once.
    def task8(self):
        
        self.cursor.execute("SELECT MIN(startTime), MAX(startTime) FROM trip;")
        startDate, endDate = self.cursor.fetchone()
        startDate = pd.to_datetime(startDate)
        endDate = pd.to_datetime(endDate)
        dateRanges = pd.date_range(start=startDate, end=endDate, freq="6D")
        threshold = 5 / 111000
        pairs = set()

        for i, periodStart in enumerate(dateRanges):
            periodEnd = periodStart + timedelta(days=6)

            query = f"""
                SELECT
                    t.taxiId,
                    t.startTime AS Time,
                    pt.longitude,
                    pt.latitude
                FROM trip t
                JOIN path p ON p.tripId = t.tripId
                JOIN point pt ON pt.PointID = p.PointID
                WHERE t.startTime >= '{periodStart.strftime('%Y-%m-%d')}'
                  AND t.startTime < '{periodEnd.strftime('%Y-%m-%d')}';
            """

            df = pd.read_sql(query, self.db_connection)

            if df.empty:
                print("No data this week")
                continue

            df["Time"] = pd.to_datetime(df["Time"])
            df.sort_values("Time", inplace=True)
            df["timeBucket"] = (df["Time"].astype("int64") // 15000000000)

            for bucket, window in df.groupby("timeBucket"):
                if len(window) < 2:
                    continue

                coords = window[["longitude", "latitude"]].to_numpy()
                taxis = window["taxiId"].to_numpy()

                tree = cKDTree(coords)
                closePairs = tree.query_pairs(r=threshold)

                for a, b in closePairs:
                    if taxis[a] != taxis[b]:
                        pairs.add(tuple(sorted((int(taxis[a]), int(taxis[b])))))

            print(f"Period {i+1}/{len(dateRanges)} done")

        result = pd.DataFrame(list(pairs), columns=["TaxiA", "TaxiB"])
        output = result.to_string(index=False)
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task8Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to file in results/task8Output.txt")

        self.db_connection.commit()

    #Find the trips that started on one calendar day and ended on the next (midnightcrossers).
    def task9(self):
        query = """
            WITH PerTrip AS (
                SELECT
                    t.TripID,
                    t.startTime AS StartTime,
                    (GREATEST(COUNT(*) - 1, 0) * 15) AS Duration
                FROM trip t
                JOIN path p ON p.TripID = t.TripID
                GROUP BY t.TripID, t.startTime
            ),
            EndCompute AS (
                SELECT
                    TripID,
                    StartTime,
                    StartTime + INTERVAL Duration SECOND AS EndTime
                FROM PerTrip
            )
            SELECT t.originalTripID
            FROM EndCompute ec
            JOIN trip t ON ec.TripID = t.TripID
            WHERE DATE(ec.StartTime) <> DATE(ec.EndTime)
            ORDER BY t.originalTripID;
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        headers = ["Trip ID"]
        output = tabulate(results, headers=headers, tablefmt="pretty")
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task9Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to file in results/task9Output.txt")

        self.db_connection.commit()
    
    #Find the trips whose start and end points are within 50 m of each other (circular
    #trips).
    def task10(self):
        query = """
            WITH tripBoundaries AS (
                SELECT 
                    TripID,
                    MIN(idx) AS firstID,
                    MAX(idx) AS lastID
                FROM path
                GROUP BY TripID
            ),
            tripPoints AS (
                SELECT
                    tb.TripID,
                    pStart.latitude  AS startLat,
                    pStart.longitude AS startLon,
                    pEnd.latitude    AS endLat,
                    pEnd.longitude   AS endLon
                FROM tripBoundaries tb
                JOIN path ps ON ps.TripID = tb.TripID AND ps.idx = tb.firstID
                JOIN point pStart ON pStart.PointID = ps.PointID
                JOIN path pe ON pe.TripID = tb.TripID AND pe.idx = tb.lastID
                JOIN point pEnd ON pEnd.PointID = pe.PointID
            )
            SELECT 
                t.originalTripID
            FROM tripPoints tp
            JOIN trip t ON tp.TripID = t.TripID
            WHERE 2 * 6371000 * ASIN(
                    SQRT(
                        POWER(SIN(RADIANS((endLat - startLat)/2)), 2) +
                        COS(RADIANS(startLat)) * COS(RADIANS(endLat)) *
                        POWER(SIN(RADIANS((endLon - startLon)/2)), 2)
                    )
                ) < 50
            ORDER BY t.originalTripID;

        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        headers = ["Trip ID"]
        output = tabulate(results, headers=headers, tablefmt="pretty")
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task10Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to file in results/task10Output.txt")

        self.db_connection.commit()

    #For each taxi, compute the average idle time between consecutive trips. List the
    #top 20 taxis with the highest average idle time.
    def task11(self):
        query = """
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
        idle_times AS (
            SELECT
                taxiId,
                endTime,
                LEAD(startTime) OVER (PARTITION BY taxiId ORDER BY startTime) AS nextStartTime
            FROM trip_times
        )
        SELECT
            taxiId,
            ROUND(AVG(TIMESTAMPDIFF(SECOND, endTime, nextStartTime))/60, 2) AS avgIdleMinutes,
            COUNT(*) AS idlePeriods
        FROM idle_times
        WHERE nextStartTime IS NOT NULL
        GROUP BY taxiId
        ORDER BY avgIdleMinutes DESC
        LIMIT 20;
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()

        headers = [desc[0] for desc in self.cursor.description]
        output = tabulate(results, headers=headers, tablefmt="pretty")
        os.makedirs("results", exist_ok=True)
        with open(os.path.join("results", "task11Output.txt"), "w", encoding="utf-8") as f:
            f.write(output)
        print("Full output written to file in results/task11Output.txt")

        self.db_connection.commit()

#Main function to run all tasks, temporarily here for easy access
def main():
    program = None
    try:
        program = Queries()
        program.task1()
        program.task2()
        program.task3()
        program.task4a()
        """program.task4b()
        program.task5()
        program.task6()
        program.task7()
        program.task9()
        program.task10()
        program.task11()
        program.task8()  # Task 8 is resource-intensive; run it last"""
    except Exception as e:
        print("ERROR: Failed to run queries:", e)
    finally:
        if program is not None:
            program.connection.close_connection()

if __name__ == "__main__":
    main()