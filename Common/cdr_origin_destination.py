from Common.hive_connection import HiveConnection
import time
from Common.helper import format_two_point_time, sql_to_string


class OriginDestination:
    def __init__(self, config):
        self.config = config
        self.hc = HiveConnection()

    def calculate_od(self):
        self.cdr_by_uid()
        self.create_od()
        self.create_od_detail()
        self.create_od_sum()

    def cdr_by_uid(self):
        provider_prefix = self.config.provider_prefix
        od_admin_unit = self.config.od_admin_unit
        cursor = self.hc.cursor
        print('########## CREATE CDR BY UID TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing.'
              .format(provider_prefix=provider_prefix))
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid'
                       .format(provider_prefix=provider_prefix))
        print('Checked and dropped {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table'
              .format(provider_prefix=provider_prefix))
        raw_sql = sql_to_string('origin_destination/create_la_cdr_all_with_ant_zone_by_uid.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)
        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        raw_sql = sql_to_string('origin_destination/insert_la_cdr_all_with_ant_zone_by_uid.sql')
        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table'
              .format(provider_prefix=provider_prefix))
        query = raw_sql.format(provider_prefix=provider_prefix, target_admin=od_admin_unit, od_date=self.config.od_date)
        cursor.execute(query)

        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING CDR BY UID TABLE ##########')

    def create_od(self):
        provider_prefix = self.config.provider_prefix
        od_admin_unit = self.config.od_admin_unit
        cursor = self.hc.cursor
        print('########## CREATE OD TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table if existing.'
              .format(provider_prefix=provider_prefix))
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od'
                       .format(provider_prefix=provider_prefix))

        print('Checked and dropped  {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing.'
              ' Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table'.format(
            provider_prefix=provider_prefix))
        timer = time.time()
        raw_sql = sql_to_string('origin_destination/create_la_cdr_all_with_ant_zone_by_uid_od.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table. Elapsed time: {time}'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table'
              .format(provider_prefix=provider_prefix))
        raw_sql = sql_to_string('origin_destination/insert_la_cdr_all_with_ant_zone_by_uid_od.sql')
        query = raw_sql.format(provider_prefix=provider_prefix, target_unit=od_admin_unit)
        cursor.execute(query)

        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table. Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING OD TABLE ##########')

    def create_od_detail(self):
        provider_prefix = self.config.provider_prefix
        cursor = self.hc.cursor
        print('########## CREATING OD DETAIL TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table if existing.'
              .format(provider_prefix=provider_prefix))
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail '
                       .format(provider_prefix=provider_prefix))

        print('Checked and dropped {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table if existing. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table'.format(
            provider_prefix=provider_prefix))

        raw_sql = sql_to_string('origin_destination/create_la_cdr_all_with_ant_zone_by_uid_od_detail.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table'.format(
            provider_prefix=provider_prefix))
        timer = time.time()
        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table'.format(
            provider_prefix=provider_prefix))
        raw_sql = sql_to_string('origin_destination/insert_la_cdr_all_with_ant_zone_by_uid_od_detail.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)

        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## CREATING OD DETAIL TABLE ##########')

    def create_od_sum(self):
        provider_prefix = self.config.provider_prefix
        cursor = self.hc.cursor
        print('########## CREATING OD SUM TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table if existing.'
              .format(provider_prefix=provider_prefix))
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum '
                       .format(provider_prefix=provider_prefix))
        print('Checked and dropped {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table if existing. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table'.format(
            provider_prefix=provider_prefix))
        raw_sql = sql_to_string('origin_destination/create_la_cdr_all_with_ant_zone_by_uid_od_sum.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)
        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table'.format(
            provider_prefix=provider_prefix))
        timer = time.time()

        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table'.format(
            provider_prefix=provider_prefix))
        raw_sql = sql_to_string('origin_destination/insert_la_cdr_all_with_ant_zone_by_uid_od_sum.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)
        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table. '
              'Elapsed time: {time} seconds'
              .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))
        raw_sql = sql_to_string('origin_destination/od_to_csv.sql')
        query = raw_sql.format(provider_prefix=provider_prefix)
        cursor.execute(query)
        print('OD Result is stored in /tmp/hive/od_result')
        print('########## FINISHED CREATING OD SUM TABLE ##########')
