import pandas as pd
from datetime import datetime, timedelta, timezone
from pinotdb import connect
import httpx


# ---- Pinot DB connection setup ----
class DBConfig:
    USERNAME = ""
    PASSWORD = ""
    HOST = ""
    PORT = 443
    PATH = "/query/sql"
    SCHEME = "https"


def create_db_connection():
    session = httpx.Client(verify=True, timeout=30000)
    connection = connect(
        host=DBConfig.HOST,
        port=DBConfig.PORT,
        path=DBConfig.PATH,
        scheme=DBConfig.SCHEME,
        username=DBConfig.USERNAME,
        password=DBConfig.PASSWORD,
        session=session
    )
    return connection


# ---- Date setup ----
today = datetime.now(timezone.utc)
start_30_days_ago = today - timedelta(days=30)
from_epoch = int(
    datetime.combine(start_30_days_ago.date(), datetime.min.time()).replace(tzinfo=timezone.utc).timestamp() * 1000)
to_epoch = int(datetime.combine(today.date() + timedelta(days=1), datetime.min.time()).replace(
    tzinfo=timezone.utc).timestamp() * 1000)


# Function to get beacons by volume category
def get_beacons_by_volume(min_scans, max_scans):
    query = f"""
    SELECT 
        beacon as beacon_id,
        COUNT(*) as total_scans
    FROM uniqode_qrcodes_upsert
    WHERE "time" >= {from_epoch} AND "time" < {to_epoch}
    GROUP BY beacon_id
    HAVING total_scans BETWEEN {min_scans} AND {max_scans}
    """
    return query


# Modified function to get data with different time windows based on volume category
def get_time_window_data(beacon_list, category):
    beacons_str = ",".join(f"'{b}'" for b in beacon_list)

    # Different time windows based on volume category
    if category == 'High Volume':
        # 5-minute intervals
        time_window = "'5:MINUTES'"
    elif category == 'Medium Volume':
        # 15-minute intervals
        time_window = "'15:MINUTES'"
    else:
        # 1-day intervals for low volume
        time_window = "'60:MINUTES'"

    query = f"""
    SELECT
        beacon as beacon_id,
        DATETIMECONVERT("time", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', {time_window}) AS bucket,
        COUNT(*) AS scan_count
    FROM uniqode_qrcodes_upsert
    WHERE "time" >= {from_epoch} 
    AND "time" < {to_epoch}
    AND beacon IN ({beacons_str})
    GROUP BY beacon_id, bucket
    ORDER BY bucket DESC
    LIMIT 100000
    """
    return query


# Function to detect anomalies
def detect_anomalies(df, category, z_score_threshold=2):
    if df.empty:
        return pd.DataFrame()

    # Convert bucket to datetime and extract date/hour
    df['bucket'] = pd.to_datetime(df['bucket'], unit='ms')
    df['date'] = df['bucket'].dt.date

    # Extract appropriate time unit based on category
    if category == 'High Volume':
        df['time_unit'] = df['bucket'].dt.hour * 12 + df['bucket'].dt.minute // 5
    elif category == 'Medium Volume':
        df['time_unit'] = df['bucket'].dt.hour * 4 + df['bucket'].dt.minute // 15
    else:
        df['time_unit'] = df['bucket'].dt.hour

    # Get the latest date
    latest_date = df['date'].max()

    # Split data
    historical_data = df[df['date'] < latest_date]
    today_data = df[df['date'] == latest_date]

    # Calculate baseline statistics
    baseline_stats = (
        historical_data.groupby(['beacon_id', 'time_unit'])['scan_count']
        .agg(['mean', 'std'])
        .reset_index()
    )

    # Merge and calculate z-scores
    merged = pd.merge(today_data, baseline_stats, on=['beacon_id', 'time_unit'], how='left')
    merged['z_score'] = (merged['scan_count'] - merged['mean']) / merged['std']
    merged['is_anomaly'] = merged['z_score'].abs() > z_score_threshold

    return merged[merged['is_anomaly']].sort_values('z_score', ascending=False)


# Volume categories
volume_categories = {
    'High Volume': (2000, 1000000),
    'Medium Volume': (250, 2000),
    'Low Volume': (100, 250)
}

# Process each volume category
connection = create_db_connection()
cursor = connection.cursor()

for category, (min_scans, max_scans) in volume_categories.items():
    print(f"\n=== {category} QR Codes ===")

    # Get beacons for this category
    beacons_query = get_beacons_by_volume(min_scans, max_scans)
    print(f"\nQuery to get {category} beacons:")
    print(beacons_query)

    cursor.execute(beacons_query)
    beacons = [row[0] for row in cursor._results]
    print(f"\nNumber of {category} beacons found: {len(beacons)}")

    if beacons:
        # Get time window data for these beacons
        time_window_query = get_time_window_data(beacons, category)
        print(f"\nQuery to get {category} data:")
        print(time_window_query)

        cursor.execute(time_window_query)
        rows = cursor._results
        columns = [desc[0] for desc in cursor.description]

        if rows:
            df = pd.DataFrame(rows, columns=columns)
            anomalies = detect_anomalies(df, category)

            print(f"\nAnomalies found for {category}: {len(anomalies)}")
            if not anomalies.empty:
                print("\nTop anomalies:")
                print(anomalies[['beacon_id', 'bucket', 'scan_count', 'mean', 'std', 'z_score']])