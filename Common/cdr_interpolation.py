from Common.hive_connection import HiveConnection
import time
from Common.helper import format_two_point_time, sql_to_string


class Interpolation:
    def __init__(self, config):
        self.config = config
        self.hc = HiveConnection()

    def calculate_interpolation(self):
        self.convert_cdr_to_array_format()
        self.create_trip_format()
        self.create_trip_24hr_padding()
        self.create_poi_relocation()
        self.create_route_interpolation()
        self.export_to_csv()

    def convert_cdr_to_array_format(self):
        provider_prefix = self.config.provider_prefix
        cursor = self.hc.cursor
        print('########## CREATE CDR BY UID ARRAY FORMAT TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_cdr_by_uid table if existing.'
              .format(provider_prefix=provider_prefix))
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cdr_by_uid'
                       .format(provider_prefix=provider_prefix))
        print('Checked and dropped {provider_prefix}_cdr_by_uid  table if existing. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_cdr_by_uid table'
              .format(provider_prefix=provider_prefix))
        raw_sql = sql_to_string('interpolation/create_cdr_by_uid.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Created {provider_prefix}_cdr_by_uid table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        raw_sql = sql_to_string('interpolation/insert_cdr_by_uid.sql')
        print('Inserting into {provider_prefix}_cdr_by_uid table'
              .format(provider_prefix=provider_prefix))
        query = raw_sql.format(provider_prefix=provider_prefix, max_size_cdr_by_uid=self.config.max_size_cdr_by_uid)
        cursor.execute(query)

        print('Inserted into {provider_prefix}_cdr_by_uid table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING CDR BY UID TABLE ##########')

    def create_trip_format(self):
        provider_prefix = self.config.provider_prefix
        cursor = self.hc.cursor
        print('########## CREATE CDR BY UID ARRAY TRIP FORMAT TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_cdr_by_uid_trip table if existing.'
              .format(provider_prefix=provider_prefix))
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cdr_by_uid_trip'
                       .format(provider_prefix=provider_prefix))
        print('Checked and dropped {provider_prefix}_cdr_by_uid_trip  table if existing. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_cdr_by_uid_trip table'
              .format(provider_prefix=provider_prefix))
        raw_sql = sql_to_string('interpolation/create_trip_format.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Created {provider_prefix}_cdr_by_uid_trip table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        raw_sql = sql_to_string('interpolation/insert_trip_format.sql')
        print('Inserting into {provider_prefix}_cdr_by_uid_trip table'
              .format(provider_prefix=provider_prefix))
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Inserted into {provider_prefix}_cdr_by_uid_trip table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING CDR BY UID TRIP FORMAT TABLE ##########')

    def create_trip_24hr_padding(self):
        provider_prefix = self.config.provider_prefix
        cursor = self.hc.cursor
        print('########## CREATE TRIP 24 HR PADDING TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_cdr_by_uid_trip_organized_array_apd table if existing.'
              .format(provider_prefix=provider_prefix))
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cdr_by_uid_trip_organized_array_apd'
                       .format(provider_prefix=provider_prefix))
        print('Checked and dropped {provider_prefix}_cdr_by_uid_trip_organized_array_apd table if existing. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_cdr_by_uid_trip_organized_array_apd table'
              .format(provider_prefix=provider_prefix))
        raw_sql = sql_to_string('interpolation/create_trip_24_hr_padding.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Created {provider_prefix}_cdr_by_uid_trip_organized_array_apd table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        raw_sql = sql_to_string('interpolation/insert_trip_24_hr_padding.sql')
        print('Inserting into {provider_prefix}_cdr_by_uid_trip_organized_array_apd table'
              .format(provider_prefix=provider_prefix))
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Inserted into {provider_prefix}_cdr_by_uid_trip_organized_array_apd table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED TRIP 24 HR PADDING TABLE ##########')

    def create_poi_relocation(self):
        provider_prefix = self.config.provider_prefix
        cursor = self.hc.cursor
        print('########## CREATE POI RELOCATION TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_cdr_by_uid_trip_realloc_array_apd table if existing.'
              .format(provider_prefix=provider_prefix))
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cdr_by_uid_trip_realloc_array_apd'
                       .format(provider_prefix=provider_prefix))
        print('Checked and dropped {provider_prefix}_cdr_by_uid_trip_realloc_array_apd table if existing. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_cdr_by_uid_trip_realloc_array_apd table'
              .format(provider_prefix=provider_prefix))
        raw_sql = sql_to_string('interpolation/create_poi_relocation.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Created {provider_prefix}_cdr_by_uid_trip_realloc_array_apd table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        raw_sql = sql_to_string('interpolation/insert_poi_relocation.sql')
        print('Inserting into {provider_prefix}_cdr_by_uid_trip_realloc_array_apd table'
              .format(provider_prefix=provider_prefix))
        query = raw_sql.format(provider_prefix=provider_prefix,
                               poi=self.config.interpolation_poi_file_location.split('/')[-1])
        cursor.execute(query)

        print('Inserted into {provider_prefix}_cdr_by_uid_trip_realloc_array_apd table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING POI RELOCATION TABLE ##########')

    def create_route_interpolation(self):
        provider_prefix = self.config.provider_prefix
        cursor = self.hc.cursor
        print('########## CREATE ROUTE INTERPOLATION TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_cdr_by_uid_trip_routing_array_apd table if existing.'
              .format(provider_prefix=provider_prefix))
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cdr_by_uid_trip_routing_array_apd'
                       .format(provider_prefix=provider_prefix))
        print('Checked and dropped {provider_prefix}_cdr_by_uid_trip_routing_array_apd table if existing. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_cdr_by_uid_trip_routing_array_apd table'
              .format(provider_prefix=provider_prefix))
        raw_sql = sql_to_string('interpolation/create_route_interpolation.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Created {provider_prefix}_cdr_by_uid_trip_routing_array_apd table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        raw_sql = sql_to_string('interpolation/insert_route_interpolation.sql')
        print('Inserting into {provider_prefix}_cdr_by_uid_trip_routing_array_apd table'
              .format(provider_prefix=provider_prefix))
        query = raw_sql.format(provider_prefix=provider_prefix,
                               max_size_interpolation=self.config.max_size_interpolation,
                               osm=self.config.interpolation_osm_file_location.split('/')[-1],
                               voronoi=self.config.interpolation_voronoi_file_location.split('/')[-1])
        cursor.execute(query)

        print('Inserted into {provider_prefix}_cdr_by_uid_trip_routing_array_apd table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED ROUTE INTERPOLATION TABLE ##########')

    def export_to_csv(self):
        provider_prefix = self.config.provider_prefix
        cursor = self.hc.cursor
        print('########## Exporting route interpolation to CSV ##########')
        timer = time.time()
        raw_sql = sql_to_string('interpolation/export_to_gps_format.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)
        print('Exported to CSV. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED EXPORTING, FILE LOCATED IN /tmp/hive/cdr_interpolation ##########')
