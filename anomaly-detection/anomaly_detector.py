import pandas as pd
from datetime import datetime, timedelta, timezone
from pinotdb import connect
import httpx

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)


class DBConfig:
    USERNAME = " "
    PASSWORD = " "
    HOST = " "
    PORT = 443
    PATH = "/query/sql"
    SCHEME = "https"


class AnomalyDetector:
    def __init__(self):
        self.connection = self._create_db_connection()
        self.today = datetime.now(timezone.utc)
        self.start_30_days_ago = self.today - timedelta(days=30)
        self.from_epoch = int(datetime.combine(self.start_30_days_ago.date(), datetime.min.time())
                              .replace(tzinfo=timezone.utc).timestamp() * 1000)
        self.to_epoch = int(datetime.combine(self.today.date() + timedelta(days=1), datetime.min.time())
                            .replace(tzinfo=timezone.utc).timestamp() * 1000)

    def _create_db_connection(self):
        session = httpx.Client(verify=True, timeout=30000)
        return connect(
            host=DBConfig.HOST,
            port=DBConfig.PORT,
            path=DBConfig.PATH,
            scheme=DBConfig.SCHEME,
            username=DBConfig.USERNAME,
            password=DBConfig.PASSWORD,
            session=session
        )

    def get_beacons_by_volume(self, min_scans, max_scans):
        """Get beacons within specified scan volume range"""
        query = f"""
        SELECT
            beacon as beacon_id,
            COUNT(*) as total_scans
        FROM uniqode_qrcodes_upsert
        WHERE "time" >= {self.from_epoch} AND "time" < {self.to_epoch}
        GROUP BY beacon_id
        HAVING total_scans BETWEEN {min_scans} AND {max_scans}
        """
        print(f"\nQuerying beacons with {min_scans}-{max_scans} scans:")
        print(query)

        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor._results

    def get_volume_data(self, beacons, window):
        """Get volume data for beacons with specified time window"""
        if not beacons:
            return None, None

        beacon_list = ",".join(f"'{b[0]}'" for b in beacons)
        query = f"""
        SELECT
            beacon as beacon_id,
            DATETIMECONVERT("time", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '{window}') AS bucket,
            HOUR(DATETIMECONVERT("time", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')) as hour_of_day,
            dayOfWeek(DATETIMECONVERT("time", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS')) as day_of_week,
            COUNT(*) AS scan_count
        FROM uniqode_qrcodes_upsert
        WHERE "time" >= {self.from_epoch}
        AND "time" < {self.to_epoch}
        AND beacon IN ({beacon_list})
        GROUP BY beacon_id, bucket, hour_of_day, day_of_week
        ORDER BY bucket DESC
        limit 10000
        """
        print(f"\nQuerying volume data with {window} window:")
        print(query)

        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor._results, [desc[0] for desc in cursor.description]

    def get_location_data(self, beacons, window):
        """Get location data for beacons"""
        if not beacons:
            return None, None

        beacon_list = ",".join(f"'{b[0]}'" for b in beacons)
        query = f"""
        SELECT
            beacon as beacon_id,
            DATETIMECONVERT("time", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '{window}') AS bucket,
            ipData_city,
            ipData_country_name,
            COUNT(*) as location_count
        FROM uniqode_qrcodes_upsert
        WHERE "time" >= {self.from_epoch}
        AND "time" < {self.to_epoch}
        AND beacon IN ({beacon_list})
        GROUP BY beacon_id, bucket, ipData_city, ipData_country_name
        HAVING location_count > 0
        ORDER BY bucket DESC
        limit 10000
        """
        print(f"\nQuerying location data with {window} window:")
        print(query)

        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor._results, [desc[0] for desc in cursor.description]

    def detect_volume_anomalies(self, volume_data, columns):
        """Detect volume-based anomalies"""
        if not volume_data:
            return pd.DataFrame()

        df = pd.DataFrame(volume_data, columns=columns)
        df['bucket'] = pd.to_datetime(df['bucket'], unit='ms')
        df['date'] = df['bucket'].dt.date
        df['hour'] = df['bucket'].dt.hour

        print("\nVolume data shape:", df.shape)
        print("Sample volume data:")
        print(df.head())

        # Get the latest date
        latest_date = df['date'].max()

        # Split data
        historical_data = df[df['date'] < latest_date]
        today_data = df[df['date'] == latest_date]

        volume_baseline = (historical_data.groupby(['beacon_id', 'hour_of_day', 'day_of_week'])['scan_count']
                           .agg(['mean', 'std'])
                           .reset_index())

        time_pattern = (
            historical_data.groupby(['beacon_id', 'hour_of_day'])
            .agg({
                'scan_count': ['count', 'mean', 'std'],
                'day_of_week': lambda x: set(x)  # Track which days this hour usually has activity
            })
            .reset_index()
        )
        time_pattern.columns = ['beacon_id', 'hour_of_day', 'frequency', 'mean_count', 'std_count', 'active_days']

        # Merge volume statistics
        volume_merged = pd.merge(
            today_data,
            volume_baseline,
            on=['beacon_id', 'hour_of_day', 'day_of_week'],
            how='left'
        )
        volume_merged['volume_z_score'] = (volume_merged['scan_count'] - volume_merged['mean']) / volume_merged['std']
        volume_merged['is_volume_anomaly'] = volume_merged['volume_z_score'].abs() > 2

        # Merge time pattern statistics
        time_merged = pd.merge(
            today_data,
            time_pattern,
            on=['beacon_id', 'hour_of_day'],
            how='left'
        )

        # Check for time pattern anomalies
        time_merged['is_new_time'] = time_merged.apply(
            lambda row: row['day_of_week'] not in row['active_days']
            if isinstance(row['active_days'], set) else False,
            axis=1
        )

        time_merged['time_z_score'] = (time_merged['scan_count'] - time_merged['mean_count']) / time_merged['std_count']
        time_merged['is_time_anomaly'] = (time_merged['is_new_time']) | (time_merged['time_z_score'].abs() > 2)

        # Combine volume and time anomalies
        final_merged = pd.merge(
            volume_merged,
            time_merged[
                ['beacon_id', 'bucket', 'is_new_time', 'time_z_score', 'is_time_anomaly', 'frequency', 'active_days']],
            on=['beacon_id', 'bucket']
        )

        # Mark as anomaly if either volume or time pattern is anomalous
        final_merged['is_anomaly'] = final_merged['is_volume_anomaly'] | final_merged['is_time_anomaly']

        # Add anomaly type classification
        final_merged['anomaly_type'] = final_merged.apply(
            lambda row: 'Both' if row['is_volume_anomaly'] and row['is_time_anomaly']
            else 'Volume' if row['is_volume_anomaly']
            else 'Time Pattern' if row['is_time_anomaly']
            else 'None',
            axis=1
        )

        return final_merged[final_merged['is_anomaly']].sort_values(['anomaly_type', 'volume_z_score', 'time_z_score'],
                                                                    ascending=False)

    def detect_location_anomalies(self, location_data, columns):
        """Detect location-based anomalies"""
        if not location_data:
            return pd.DataFrame()

        df = pd.DataFrame(location_data, columns=columns)
        df['bucket'] = pd.to_datetime(df['bucket'], unit='ms')
        df['date'] = df['bucket'].dt.date

        print("\nLocation data shape:", df.shape)
        print("Sample location data:")
        print(df.head())

        # Get the latest date
        latest_date = df['date'].max()

        # Split data
        historical_data = df[df['date'] < latest_date]
        today_data = df[df['date'] == latest_date]

        # Create sets of historical locations per beacon
        historical_locations = historical_data.groupby('beacon_id').agg({
            'ipData_city': lambda x: set(x),
            'ipData_country_name': lambda x: set(x)
        })

        # Find new locations
        location_anomalies = []
        for _, row in today_data.iterrows():
            beacon_id = row['beacon_id']
            if beacon_id in historical_locations.index:
                hist_cities = historical_locations.loc[beacon_id, 'ipData_city']
                hist_countries = historical_locations.loc[beacon_id, 'ipData_country_name']

                if (row['ipData_city'] not in hist_cities or
                        row['ipData_country_name'] not in hist_countries):
                    location_anomalies.append({
                        'beacon_id': beacon_id,
                        'timestamp': row['bucket'],
                        'new_city': row['ipData_city'],
                        'new_country': row['ipData_country_name'],
                        'scan_count': row['location_count']
                    })

        return pd.DataFrame(location_anomalies)

    def run_analysis(self):
        """Run complete anomaly detection analysis"""
        # Analyze Low Volume QRs (100-250 scans)
        low_volume_beacons = self.get_beacons_by_volume(100, 250)
        print(f"\nLow volume beacons found: {len(low_volume_beacons)}")

        if low_volume_beacons:
            # Volume anomalies
            volume_data, volume_cols = self.get_volume_data(low_volume_beacons, '1:HOURS')
            volume_anomalies = self.detect_volume_anomalies(volume_data, volume_cols)

            print("\nLow Volume - Volume Anomalies found:", len(volume_anomalies))
            if not volume_anomalies.empty:
                print("\nTop volume anomalies:")
                print(volume_anomalies[
                          ['beacon_id', 'bucket', 'scan_count', 'mean', 'std', 'volume_z_score', 'time_z_score',
                           'is_new_time', 'anomaly_type']])

            # Location anomalies
            location_data, location_cols = self.get_location_data(low_volume_beacons, '1:HOURS')
            location_anomalies = self.detect_location_anomalies(location_data, location_cols)

            print("\nLow Volume - Location Anomalies found:", len(location_anomalies))
            if not location_anomalies.empty:
                print("\nLocation anomalies:")
                print(location_anomalies)

        # Analyze Medium Volume QRs (250-2000 scans)
        medium_volume_beacons = self.get_beacons_by_volume(250, 2000)
        print(f"\nMedium volume beacons found: {len(medium_volume_beacons)}")

        if medium_volume_beacons:
            # Volume anomalies
            volume_data, volume_cols = self.get_volume_data(medium_volume_beacons, '15:MINUTES')
            volume_anomalies = self.detect_volume_anomalies(volume_data, volume_cols)

            print("\nMedium Volume - Volume Anomalies found:", len(volume_anomalies))
            if not volume_anomalies.empty:
                print("\nTop volume anomalies:")
                print(volume_anomalies[
                          ['beacon_id', 'bucket', 'scan_count', 'mean', 'std', 'volume_z_score', 'time_z_score',
                           'is_new_time', 'anomaly_type']])

            # Location anomalies
            location_data, location_cols = self.get_location_data(medium_volume_beacons, '15:MINUTES')
            location_anomalies = self.detect_location_anomalies(location_data, location_cols)

            print("\nMedium Volume - Location Anomalies found:", len(location_anomalies))
            if not location_anomalies.empty:
                print("\nLocation anomalies:")
                print(location_anomalies)

        # Analyze High Volume QRs (>2000 scans)
        high_volume_beacons = self.get_beacons_by_volume(2000, 1000000)
        print(f"\nHigh volume beacons found: {len(high_volume_beacons)}")

        if high_volume_beacons:
            # Volume anomalies
            volume_data, volume_cols = self.get_volume_data(high_volume_beacons, '5:MINUTES')
            volume_anomalies = self.detect_volume_anomalies(volume_data, volume_cols)

            print("\nHigh Volume - Volume Anomalies found:", len(volume_anomalies))
            if not volume_anomalies.empty:
                print("\nTop volume anomalies:")
                print(volume_anomalies[
                          ['beacon_id', 'bucket', 'scan_count', 'mean', 'std', 'volume_z_score', 'time_z_score',
                           'is_new_time', 'anomaly_type']])

            # Location anomalies
            location_data, location_cols = self.get_location_data(high_volume_beacons, '5:MINUTES')
            location_anomalies = self.detect_location_anomalies(location_data, location_cols)

            print("\nHigh Volume - Location Anomalies found:", len(location_anomalies))
            if not location_anomalies.empty:
                print("\nLocation anomalies:")
                print(location_anomalies)


def main():
    detector = AnomalyDetector()
    detector.run_analysis()


if __name__ == "__main__":
    main()