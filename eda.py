
import pandas as pd
import matplotlib.pyplot as plt
import ast
import seaborn as sns
from geopy.distance import geodesic


def read_porto_csv(filepath='porto.csv'):
    """
    Reads the porto.csv file and returns a pandas DataFrame.
    """
    df = pd.read_csv(filepath)
    return df

def calculate_time_from_start(row):
    """
    Calculate the time in minutes from the start of the trip.
    """
    start_time = pd.to_datetime(row['TIMESTAMP'], unit='s')
    num_points = len(ast.literal_eval(row['POLYLINE']))
    trip_time_seconds = num_points * 15
    end_time = start_time + pd.Timedelta(seconds=trip_time_seconds)
    return end_time

def calculate_start_end_meters(row):
    """
    Calculate the difference between start and end points in meters from the POLYLINE data.
    """
    polyline_points = ast.literal_eval(row['POLYLINE'])
    start = polyline_points[0] if polyline_points else None
    end = polyline_points[-1] if polyline_points else None
    difference_meters = geodesic((start[1], start[0]), (end[1], end[0])).meters if start and end else None
    return difference_meters


### First looks at data ###

# Whole df
df = read_porto_csv()

# First looks
print(df.columns)
print(df.head(10))
print(df.info())

# Aspects of first row
print("Start Time: ", pd.to_datetime(df.iloc[0]['TIMESTAMP'], unit='s'))
print("End Time: ", calculate_time_from_start(df.iloc[0]))
print("Start and End Meters: ", calculate_start_end_meters(df.iloc[0]))

### Specific EDA ###

# Find unique trip IDs
unique_trip_ids = df['TRIP_ID'].nunique()
print(f"Number of TRIP_IDs: {len(df['TRIP_ID'])}")
print(f"Number of unique TRIP_IDs: {unique_trip_ids}")

# Call type distribution
sns.countplot(x='CALL_TYPE', data=df)
plt.show()

# Day type distribution
sns.countplot(x='DAY_TYPE', data=df)
plt.show()

# Holiday checks
selected_holidays = {
    (12, 31),  # New year's Eve
    (1, 1),    # New Year’s Day
    (4, 25),   # Freedom Day
    (5, 1),    # Labour Day
    (6, 10),   # Portugal Day
    (8, 15),   # Assumption of Mary
    (10, 5),   # Republic Day
    (11, 1),   # All Saints’ Day
    (12, 1),   # Restoration of Independence
    (12, 8),   # Immaculate Conception
    (12, 24),  # Christmas eve
    (12, 25),  # Christmas Day
}

df['start_time'] = pd.to_datetime(df['TIMESTAMP'], unit='s')
df['start_month'], df['start_day'] = df['start_time'].dt.month, df['start_time'].dt.day

df['is_holiday'] = df.apply(lambda row: (row.start_month, row.start_day) in selected_holidays, axis=1)

sns.countplot(x='is_holiday', data=df)
plt.xlabel('Is fixed holiday or day before')
plt.ylabel('Number of Trips')
plt.title('Distribution of Trips on holidays')
plt.show()

# Get the clients with more than a specific number of calls
call_counts = df['ORIGIN_CALL'].value_counts()
ids_with_multiple_calls = call_counts[call_counts > 50].index
count = len(ids_with_multiple_calls)
print(f"Number of IDs with more than 50 ORIGIN_CALL: {count}")

# Missing data analysis
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
df_missing = df[df['MISSING_DATA'] == True]

print("Missing data rows length:",len(df_missing))
print(df_missing)
missing_polyline_points = ast.literal_eval(df_missing.iloc[0]['POLYLINE'])

lons = [point[0] for point in missing_polyline_points]
lats = [point[1] for point in missing_polyline_points]

plt.plot(lons, lats, marker='o')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Polyline Points of First Missing Row')
plt.show()


#TASKS

# Unique TAXI_IDs (task 2-1)
unique_count = df['TAXI_ID'].nunique()
print(f"Number of unique TAXI_IDs: {unique_count}")

# Average number of trips per TAXI_ID (task 2-2)
trip_counts = df['TAXI_ID'].value_counts()
average_trips = trip_counts.mean()
print(f"Average number of trips per TAXI_ID: {average_trips}")

# The 20 TAXI_IDs with the most trips (task 2-3)
top_taxis = df['TAXI_ID'].value_counts().head(20).index.tolist()
print("TAXI_IDs with the 20 most trips:", top_taxis)

"""
Task 2-7, should be cleaned somehow either by removing or inferring
Task 2-7 takes long to run, result shown in screenshot

count = 0
chunksize = 10000  # Adjust based on your memory

for chunk in pd.read_csv('porto/porto/porto.csv', chunksize=chunksize):
    count += chunk['POLYLINE'].apply(lambda x: len(ast.literal_eval(x)) < 3).sum()

print(f"Number of rows with less than 3 points in POLYLINE: {count}")
"""

# For task 8
sns.kdeplot(data=df, x='TIMESTAMP')
plt.xlabel('Start time')
plt.ylabel('Density')
plt.show()

print("Start Time: ", pd.to_datetime(df.iloc[0]['TIMESTAMP'], unit='s'))
print("End Time: ", calculate_time_from_start(df.iloc[0]))

# Integrity
nulls = df.isna().sum()
sns.barplot(data=nulls)
plt.xticks(rotation=30)
plt.xlabel('Column')
plt.ylabel('Number of None/null/N/A values')
plt.show()
