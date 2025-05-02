from pinotdb import connect
import httpx
import csv
import os

# ---- Pinot DB connection setup ----
class DBConfig:
    USERNAME = ""
    PASSWORD = ""
    HOST = " "
    PORT = 443
    PATH = "/query/sql"
    SCHEME = "https"

class MultiOrgCsvExport:
    def __init__(self, from_epoch, to_epoch, organizations, output_file='pinot_export.csv'):
        self.connection = self._create_db_connection()
        self.from_epoch = from_epoch
        self.to_epoch = to_epoch
        self.organizations = organizations
        self.output_file = output_file

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

    def csv_data(self):
        cursor = self.connection.cursor()
        orgs = ','.join(f"'{org}'" for org in self.organizations)

        # Get total number of rows
        count_query = f"""
            SELECT count(*) as total_scans
            FROM uniqode_qrcodes_upsert
            WHERE "time" BETWEEN '{self.from_epoch}' AND '{self.to_epoch}'
            AND organization IN ({orgs})
        """
        cursor.execute(count_query)
        total_rows = cursor.fetchall()[0][0]

        print(f"Total rows to export: {total_rows}")

        header_written = os.path.exists(self.output_file)

        with open(self.output_file, mode='a', newline='', encoding='utf-8') as csvfile:
            writer = None
            for i in range(0, total_rows, 1000):
                data_query = f"""
                    SELECT mappable_id, "time", fingerprint, beacon, beaconName, beaconURL, organization, organizationName, eventInfo_userAgent, eventInfo_ipAddress, tagNames,
                    CASE WHEN gpsData_location_lat <> 'null' THEN 'GPS' ELSE 'IP' END AS locationSource,
                    CASE WHEN gpsData_locality <> 'null' THEN gpsData_locality WHEN gpsData_administrative_area_level_2 <> 'null' THEN gpsData_administrative_area_level_2 ELSE ipData_city END AS city,
                    CASE WHEN gpsData_postal_code <> 'null' THEN gpsData_postal_code ELSE ipData_postal END as postal_code,
                    CASE WHEN gpsData_administrative_area_level_1 <> 'null' THEN gpsData_administrative_area_level_1 ELSE ipData_region END as region,
                    CASE WHEN gpsData_country <> 'null' THEN gpsData_country ELSE ipData_country_name END as country,
                    CASE WHEN gpsData_location_lat <> 'null' THEN gpsData_location_lat ELSE ipData_location_lat END as lat,
                    CASE WHEN gpsData_location_lon <> 'null' THEN gpsData_location_lon ELSE ipData_location_lon END as lon
                    FROM upsert_table_name
                    WHERE "time" BETWEEN '{self.from_epoch}' AND '{self.to_epoch}'
                    AND organization IN ({orgs})
                    ORDER BY "time"
                    LIMIT {i}, 1000
                """
                cursor.execute(data_query)
                rows = cursor.fetchall()
                if not rows:
                    break
                # Write headers only once
                if not header_written:
                    columns = [col[0] for col in cursor.description]
                    writer = csv.writer(csvfile)
                    writer.writerow(columns)
                    header_written = True
                if writer is None:
                    writer = csv.writer(csvfile)
                writer.writerows(rows)
                print(f"Exported rows: {i + len(rows)} / {total_rows}")

        print(f"Data export completed to {self.output_file}")

# Example usage:
exporter = MultiOrgCsvExport(from_epoch=1735689600000, to_epoch=1743465599999, organizations=["123", "345"])
exporter.csv_data()
