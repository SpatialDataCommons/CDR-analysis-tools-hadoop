from impala.dbapi import connect
from helper import json_file_to_object, get_admin_units_from_mapping, format_two_point_time, sql_to_string
import os
import time

class HiveConnector:
    def __init__(self, config):
        # username is mandatory
        self.conn = connect(config.host, config.port, user='rsstudent', auth_mechanism='PLAIN')
        self.cursor = self.conn.cursor()

    def initialize(self, config):
        for command in json_file_to_object('initial_hive_commands.json')['hive_commands']:
            self.cursor.execute(command)
        if not os.path.exists(config.output_report_location):
            os.makedirs(config.output_report_location)
        if not os.path.exists(config.output_graph_location):
            os.makedirs(config.output_graph_location)

    def create_tables(self, config, data):
        self.import_cell_tower_data_raw(config, data)
        self.preprocess_cell_tower_data(config, data)
        #
        admins = get_admin_units_from_mapping(config.cdr_cell_tower)

        for admin in admins:
            self.cell_tower_data_admin(config, admin)
        #
        self.import_raw(config, data)
        self.preprocess_data(config, data)
        self.consolidate_table(config, data)

        # pass
        # self.cdr_by_uid(config)
        # self.create_od(config)
        # self.create_od_detail()
        # self.create_od_sum()

    def import_cell_tower_data_raw(self, config, data):
        print('########## IMPORT RAW MAPPING TABLE ##########')
        print('Checking and dropping raw mapping table if existing.')
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_raw'
                            .format(provider_prefix=config.provider_prefix))
        print('Checked and dropped raw mapping table if existing. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating raw mapping table')
        raw_query = sql_to_string('cdr_and_mapping/create_raw_mapping.sql')
        query = raw_query.format(provider_prefix=config.provider_prefix,
                                 arg_raw=', '.join(data.arg_cell_raw),
                                 field_delimiter=config.input_cell_tower_delimiter,
                                 have_header=config.input_cell_tower_have_header)
        self.cursor.execute(query)
        print('Created raw mapping table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()

        if len(config.input_cell_tower_files) < 1:
            print('Please check the input_cell_tower_files field in config.json and make sure the file is valid.')
            return
        elif len(config.input_cell_tower_files) == 1:
            self.cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=config.hadoop_data_path, hadoop_data_file=config.input_cell_tower_files[0]) +
                "overwrite into table {provider_prefix}_cell_tower_data_raw".format(
                    provider_prefix=config.provider_prefix)
            )
        else:
            self.cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=config.hadoop_data_path, hadoop_data_file=config.input_cell_tower_files[0]) +
                "overwrite into table {provider_prefix}_cell_tower_data_raw".format(
                    provider_prefix=config.provider_prefix)
            )
            for i in range(1, len(config.input_cell_tower_files)):
                self.cursor.execute(
                    "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                    .format(hadoop_data_path=config.hadoop_data_path, hadoop_data_file=config.input_cell_tower_files[i]) +
                    "into table {provider_prefix}_cell_tower_data_raw".format(provider_prefix=config.provider_prefix)
                )
        print('Imported to raw mapping table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        print('########## FINISHED IMPORTING TO RAW MAPPING TABLE ##########')

    def import_raw(self, config, data):
        print('########## IMPORT RAW TABLE ##########')
        print('Checking and dropping raw table if existing.')
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_raw'
                            .format(provider_prefix=config.provider_prefix))
        print('Checked and dropped raw mapping table if existing. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating raw table')
        raw_sql = sql_to_string('cdr_and_mapping/create_raw_cdr.sql')
        query = raw_sql.format(cell_tower_header=config.input_cell_tower_have_header,
                               provider_prefix=config.provider_prefix,
                               arg_raw=', '.join(data.arg_cdr_raw),
                               field_delimiter=config.input_delimiter)
        self.cursor.execute(query)

        print('Created raw table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Importing to raw table')
        if len(config.input_files) < 1:
            'Please check the input_files field in config.json and make sure the file is valid.'
            return
        elif len(config.input_files) == 1:
            self.cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=config.hadoop_data_path, hadoop_data_file=config.input_files[0]) +
                "overwrite into table {provider_prefix}_raw".format(provider_prefix=config.provider_prefix)
            )
        else:
            self.cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=config.hadoop_data_path, hadoop_data_file=config.input_files[0]) +
                "overwrite into table {provider_prefix}_raw".format(provider_prefix=config.provider_prefix)
            )
            for i in range(1, len(config.input_files)):
                self.cursor.execute(
                    "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                    .format(hadoop_data_path=config.hadoop_data_path, hadoop_data_file=config.input_files[i]) +
                    "into table {provider_prefix}_raw".format(provider_prefix=config.provider_prefix)
                )
        print('Imported to raw table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        print('########## IMPORT RAW TABLE COMPLETED ##########')
        
    def cell_tower_data_admin(self, config, admin):
        print('########## CREATE MAPPING ADMIN TABLE ##########')
        if config.check_invalid_lat_lng:
            check_lat_lng = 'and (latitude != 0 or longitude != 0) and latitude is not NULL and longitude is not NULL'
        else:
            check_lat_lng = ''
        print('Checking and dropping mapping {admin} table if existing.'.format(admin=admin))
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_{admin}'.format(
            provider_prefix=config.provider_prefix, admin=admin))
        print('Check and drop mapping {admin} table if existing. Elapsed time: {time} seconds'.format(admin=admin, time=format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating mapping {admin} table'.format(admin=admin))
        raw_sql = sql_to_string('cdr_and_mapping/create_mapping_admin.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix, admin=admin)
        self.cursor.execute(query)
        print('Created mapping {admin} table. Elapsed time: {time} seconds'.format(admin=admin, time=format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Inserting into mapping {} table'.format(admin))
        raw_sql = sql_to_string('cdr_and_mapping/insert_mapping_admin.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix, admin=admin, check_lat_lng=check_lat_lng)
        self.cursor.execute(query)
        print('Inserted into mapping {admin} table. Elapsed time: {time} seconds'.format(admin=admin, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING MAPPING ADMIN TABLE ##########')

    def preprocess_cell_tower_data(self, config, data):
        print('########## CREATE PREPROCESS MAPPING TABLE ##########')
        if config.check_duplicate:
            distinct = 'distinct'
        else:
            distinct = ''
        print('Checking and dropping preprocess mapping table if existing.')
        timer = time.time()

        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_preprocess'.format(
            provider_prefix=config.provider_prefix))
        print('Checked and dropped preprocess mapping table if existing. Elapsed time: {} seconds'.format(
            format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating preprocess mapping table')
        raw_sql = sql_to_string('cdr_and_mapping/create_preprocess_mapping.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix,
                               arg_create=', '.join(data.arg_cell_create))
        self.cursor.execute(query)
        print('Created mapping preprocess table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()
        # need username to get privilege

        print('Inserting into preprocess mapping table')
        raw_sql = sql_to_string('cdr_and_mapping/insert_preprocess_mapping.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix, distinct=distinct, arg=', '.join(data.arg_cell_map))
        self.cursor.execute(query)
        print('Inserted into preprocess mapping table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING PREPROCESS MAPPING TABLE ##########')

    def preprocess_data(self, config, data):
        print('########## CREATE PREPROCESS CDR TABLE ##########')
        if config.check_duplicate:
            distinct = 'distinct'
        else:
            distinct = ''

        print('Checking and dropping preprocess cdr table if existing.')
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_preprocess'.format(provider_prefix=config.provider_prefix))
        print('Checked and dropped preprocess cdr table if existing. Elapsed time: {} seconds'.format(
            format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating preprocess cdr table.')
        raw_sql = sql_to_string('cdr_and_mapping/create_preprocess_cdr.sql')
        query = raw_sql.format(args=', '.join(data.arg_cdr_prep), provider_prefix=config.provider_prefix)
        self.cursor.execute(query)
        print('Created preprocess cdr table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()


        print('Inserting into preprocess table')
        print('Columns in preprocess table mapped: ' + ', '.join(data.arg_cdr_map))
        raw_sql = sql_to_string('cdr_and_mapping/insert_preprocess_cdr.sql')
        query = raw_sql.format(distinct=distinct, arg=', '.join(data.arg_cdr_map), provider_prefix=config.provider_prefix)
        self.cursor.execute(query)
        print('Inserted into preprocess cdr table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING PREPROCESS CDR TABLE ##########')

    def consolidate_table(self, config, data):
        print('########## CREATE CONSOLIDATE CDR TABLE ##########')
        print('Checking and dropping consolidate cdr table if existing.')
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_consolidate_data_all'.format(
            provider_prefix=config.provider_prefix))
        print('Checked and dropped preprocess cdr table if existing. Elapsed time: {} seconds'.format(
            format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating consolidate table')
        raw_sql = sql_to_string('cdr_and_mapping/create_consolidate_cdr.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix, arg_prep=' ,'.join(data.arg_cdr_prep))
        self.cursor.execute(query)
        print('Created consolidate cdr table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Columns in consolidatie table: ' + ', '.join(data.arg_cdr_con))
        print('Inserting into the consolidate table')
        raw_sql = sql_to_string('cdr_and_mapping/insert_consolidate_cdr.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix, arg_con=', '.join(data.arg_cdr_con))
        self.cursor.execute(query)
        print('Inserted into consolidate cdr table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING CONSOLIDATE CDR TABLE ##########')

    def cdr_by_uid(self, config):
        print('########## CREATE CDR BY UID TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing.'
              .format(provider_prefix=config.provider_prefix))
        self.cursor.execute('DROP TABLE IF EXISTS la_cdr_all_with_ant_zone_by_uid')
        print('Checked and dropped {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing. Elapsed time: {time} seconds'
            .format(provider_prefix=config.provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table'.format(provider_prefix=config.provider_prefix))
        raw_sql = sql_to_string('origin_destination/create_la_cdr_all_with_ant_zone_by_uid.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix)
        self.cursor.execute(query)

        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        raw_sql = sql_to_string('origin_destination/insert_la_cdr_all_with_zone_by_uid.sql')
        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table'.format(provider_prefix=config.provider_prefix))
        query = raw_sql.format(provider_prefix=config.provider_prefix)
        self.cursor.execute(query)

        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING CDR BY UID TABLE ##########')

    def create_od(self, config):
        print('########## CREATE OD TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing.'
              .format(provider_prefix=config.provider_prefix))
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od'
                            .format(provider_prefix=config.provider_prefix))

        print('Checked and dropped  {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing. Elapsed time: {} seconds'
            .format(provider_prefix=config.provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table'.format(
            provider_prefix=config.provider_prefix))

        raw_sql = sql_to_string('origin_destination/create_la_cdr_all_with_ant_zone_by_uid_od.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix)
        self.cursor.execute(query)

        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table'.format(
            provider_prefix=config.provider_prefix))
        timer = time.time()
        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table'.format(

            provider_prefix=config.provider_prefix))
        raw_sql =  sql_to_string('origin_destination/insert_la_cdr_all_with_ant_zone_by_uid_od.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix, target_unit=config.od_admin_unit)
        self.cursor.execute(query)

        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING OD TABLE ##########')

    def create_od_detail(self, config):
        print('########## CREATING OD DETAIL TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table if existing.'
              .format(provider_prefix=config.provider_prefix))
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail '
                            .format(provider_prefix=config.provider_prefix))

        print('Checked and dropped {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table if existing. Elapsed time: {} seconds'
            .format(provider_prefix=config.provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table'.format(
            provider_prefix=config.provider_prefix))

        raw_sql = sql_to_string('origin_destination/create_la_cdr_all_with_ant_zone_by_uid_od_detail.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix)
        self.cursor.execute(query)

        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table'.format(
            provider_prefix=config.provider_prefix))
        timer = time.time()
        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table'.format(
            provider_prefix=config.provider_prefix))
        raw_sql = sql_to_string('origin_destination/create_la_cdr_all_with_ant_zone_by_uid_od_detail')
        query = raw_sql.format(provider_prefix=config.provider_prefix)
        self.cursor.execute(query)

        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=format_two_point_time(timer, time.time())))
        print('########## CREATING OD DETAIL TABLE ##########')

    def create_od_sum(self, config):
        print('########## CREATING OD SUM TABLE ##########')
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table if existing.'
              .format(provider_prefix=config.provider_prefix))
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum '
                            .format(provider_prefix=config.provider_prefix))
        print(
            'Checked and dropped {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table if existing. Elapsed time: {} seconds'
            .format(provider_prefix=config.provider_prefix, time=format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table'.format(
            provider_prefix=config.provider_prefix))
        raw_sql = sql_to_string('origin_destination/create_la_cdr_all_with_ant_zone_by_uid_od_sum.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix)
        self.cursor.execute(query)
        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table'.format(
            provider_prefix=config.provider_prefix))
        timer = time.time()

        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table'.format(
            provider_prefix=config.provider_prefix))
        raw_sql = sql_to_string('origin_destination/insert_la_cdr_all_with_ant_zone_by_uid_od_sum.sql')
        query = raw_sql.format(provider_prefix=config.provider_prefix)
        self.cursor.execute(query)
        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=format_two_point_time(timer, time.time())))
        raw_sql = sql_to_string('origin_destination/od_to_csv.sql')
        self.cursor.execute(raw_sql)

        print('########## FINISHED CREATING OD SUM TABLE ##########')