from Common.hive_connection import HiveConnection
from Common.helper import json_file_to_object, get_admin_units_from_mapping, format_two_point_time, sql_to_string
import os
import time


class HiveTableCreator:
    def __init__(self, config, data=''):
        self.config = config
        self.data = data
        self.hc = HiveConnection()

    def initialize(self, init_cmd_file):
        print('########## Initilizing Hive ##########')
        timer = time.time()
        output_report_location = self.config.output_report_location
        output_graph_location = self.config.output_graph_location
        cursor = self.hc.cursor
        for command in json_file_to_object(init_cmd_file)['hive_commands']:
            if command.startswith('use'):
                command = command.format(db_name=self.config.db_name)
            elif '{poi_location}' in command:
                command = command.format(poi_location=self.config.interpolation_poi_file_location)
            elif '{osm_location}' in command:
                command = command.format(osm_location=self.config.interpolation_osm_file_location)
            elif '{voronoi_location}' in command:
                command = command.format(voronoi_location=self.config.interpolation_voronoi_file_location)
            cursor.execute(command)
        if not os.path.exists(output_report_location):
            os.makedirs(output_report_location)
        if not os.path.exists(output_graph_location):
            os.makedirs(output_graph_location)
        print('########## Done. Time elapsed: {} seconds ##########'.format(format_two_point_time(timer, time.time())))

    def create_tables(self):
        print('########## Creating Tables ##########')
        timer = time.time()
        self.import_cell_tower_data_raw()
        self.preprocess_cell_tower_data()
        admins = get_admin_units_from_mapping(self.config.cdr_cell_tower)
        for admin in admins:
            self.cell_tower_data_admin(admin)
        self.import_raw()
        self.preprocess_data()
        self.consolidate_table()
        print('########## Done create all tables. Time elapsed: {} seconds ##########'.format(
            format_two_point_time(timer, time.time())))

    def import_cell_tower_data_raw(self):
        provider_prefix = self.config.provider_prefix
        arg_cell_raw = self.data.arg_cell_raw
        input_cell_tower_delimiter = self.config.input_cell_tower_delimiter
        input_cell_tower_have_header = self.config.input_cell_tower_have_header
        input_cell_tower_files = self.config.input_cell_tower_files
        hadoop_data_path = self.config.hadoop_data_path
        cursor = self.hc.cursor
        print('########## IMPORT RAW MAPPING TABLE ##########')
        print('Checking and dropping raw mapping table if existing.')
        timer = time.time()
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_raw'
                            .format(provider_prefix=provider_prefix))
        print('Checked and dropped raw mapping table if existing. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating raw mapping table')
        raw_query = sql_to_string('cdr_and_mapping/create_raw_mapping.sql')
        query = raw_query.format(provider_prefix=provider_prefix,
                                 arg_raw=', '.join(arg_cell_raw),
                                 field_delimiter=input_cell_tower_delimiter,
                                 have_header=input_cell_tower_have_header)
        cursor.execute(query)
        print('Created raw mapping table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()

        if len(input_cell_tower_files) < 1:
            print('Please check the input_cell_tower_files field in config.json and make sure the file is valid.')
            return
        elif len(input_cell_tower_files) == 1:
            cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=hadoop_data_path, hadoop_data_file=input_cell_tower_files[0]) +
                "overwrite into table {provider_prefix}_cell_tower_data_raw".format(
                    provider_prefix=provider_prefix)
            )
        else:
            cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=hadoop_data_path, hadoop_data_file=input_cell_tower_files[0]) +
                "overwrite into table {provider_prefix}_cell_tower_data_raw".format(
                    provider_prefix=provider_prefix)
            )
            for i in range(1, len(input_cell_tower_files)):
                cursor.execute(
                    "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                    .format(hadoop_data_path=hadoop_data_path, hadoop_data_file=input_cell_tower_files[i]) +
                    "into table {provider_prefix}_cell_tower_data_raw".format(provider_prefix=provider_prefix)
                )
        print('Imported to raw mapping table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        print('########## FINISHED IMPORTING TO RAW MAPPING TABLE ##########')

    def import_raw(self):
        provider_prefix = self.config.provider_prefix
        hadoop_data_path = self.config.hadoop_data_path
        input_cell_tower_have_header = self.config.input_cell_tower_have_header
        arg_cdr_raw = self.data.arg_cdr_raw
        input_files = self.config.input_files
        input_delimiter = self.config.input_delimiter
        cursor = self.hc.cursor
        print('########## IMPORT RAW TABLE ##########')
        print('Checking and dropping raw table if existing.')
        timer = time.time()
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_raw'
                            .format(provider_prefix=provider_prefix))
        print('Checked and dropped raw mapping table if existing. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating raw table')
        raw_sql = sql_to_string('cdr_and_mapping/create_raw_cdr.sql')
        query = raw_sql.format(cell_tower_header=input_cell_tower_have_header,
                               provider_prefix=provider_prefix,
                               arg_raw=', '.join(arg_cdr_raw),
                               field_delimiter=input_delimiter)
        cursor.execute(query)
        print('Created raw table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        timer = time.time()
        print('Importing to raw table')
        if len(input_files) < 1:
            'Please check the input_files field in config.json and make sure the file is valid.'
            return
        elif len(input_files) == 1:
            cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=hadoop_data_path, hadoop_data_file=input_files[0]) +
                "overwrite into table {provider_prefix}_raw".format(provider_prefix=provider_prefix)
            )
        else:
            cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=hadoop_data_path, hadoop_data_file=input_files[0]) +
                "overwrite into table {provider_prefix}_raw".format(provider_prefix=provider_prefix)
            )
            for i in range(1, len(input_files)):
                cursor.execute(
                    "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                    .format(hadoop_data_path=hadoop_data_path, hadoop_data_file=input_files[i]) +
                    "into table {provider_prefix}_raw".format(provider_prefix=provider_prefix)
                )
        print('Imported to raw table. Elapsed time: {} seconds'.format(format_two_point_time(timer, time.time())))
        print('########## IMPORT RAW TABLE COMPLETED ##########')
        
    def cell_tower_data_admin(self, admin):
        provider_prefix = self.config.provider_prefix
        check_invalid_lat_lng = self.config.check_invalid_lat_lng
        cursor = self.hc.cursor

        print('########## CREATE MAPPING ADMIN TABLE ##########')
        if check_invalid_lat_lng:
            check_lat_lng = 'and (latitude != 0 or longitude != 0) and latitude is not NULL and longitude is not NULL'
        else:
            check_lat_lng = ''
        print('Checking and dropping mapping {admin} table if existing.'.format(admin=admin))
        timer = time.time()
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_{admin}'.format(
            provider_prefix=provider_prefix, admin=admin))
        print('Check and drop mapping {admin} table if existing. Elapsed time: {time} seconds'
              .format(admin=admin, time=format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating mapping {admin} table'.format(admin=admin))
        raw_sql = sql_to_string('cdr_and_mapping/create_mapping_admin.sql')
        query = raw_sql.format(provider_prefix=provider_prefix, admin=admin)
        cursor.execute(query)
        print('Created mapping {admin} table. Elapsed time: {time} seconds'
              .format(admin=admin, time=format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Inserting into mapping {} table'.format(admin))
        raw_sql = sql_to_string('cdr_and_mapping/insert_mapping_admin.sql')
        query = raw_sql.format(provider_prefix=provider_prefix, admin=admin, check_lat_lng=check_lat_lng)
        cursor.execute(query)
        print('Inserted into mapping {admin} table. Elapsed time: {time} seconds'
              .format(admin=admin, time=format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING MAPPING ADMIN TABLE ##########')

    def preprocess_cell_tower_data(self):
        provider_prefix = self.config.provider_prefix
        check_duplicate = self.config.check_duplicate
        arg_cell_create = self.data.arg_cell_create
        arg_cell_map = self.data.arg_cell_map
        cursor = self.hc.cursor
        print('########## CREATE PREPROCESS MAPPING TABLE ##########')
        if check_duplicate:
            distinct = 'distinct'
        else:
            distinct = ''
        print('Checking and dropping preprocess mapping table if existing.')
        timer = time.time()

        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_preprocess'.format(
            provider_prefix=provider_prefix))
        print('Checked and dropped preprocess mapping table if existing. Elapsed time: {} seconds'.format(
            format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating preprocess mapping table')
        raw_sql = sql_to_string('cdr_and_mapping/create_preprocess_mapping.sql')
        query = raw_sql.format(provider_prefix=provider_prefix,
                               arg_create=', '.join(arg_cell_create))
        cursor.execute(query)
        print('Created mapping preprocess table. Elapsed time: {} seconds'
              .format(format_two_point_time(timer, time.time())))
        timer = time.time()
        # need username to get privilege

        print('Inserting into preprocess mapping table')
        raw_sql = sql_to_string('cdr_and_mapping/insert_preprocess_mapping.sql')
        query = raw_sql.format(provider_prefix=provider_prefix, distinct=distinct, arg=', '.join(arg_cell_map))
        cursor.execute(query)
        print('Inserted into preprocess mapping table. Elapsed time: {} seconds'
              .format(format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING PREPROCESS MAPPING TABLE ##########')

    def preprocess_data(self):
        provider_prefix = self.config.provider_prefix
        check_duplicate = self.config.check_duplicate
        arg_cdr_prep = self.data.arg_cdr_prep
        arg_cdr_map = self.data.arg_cdr_map
        cursor = self.hc.cursor

        print('########## CREATE PREPROCESS CDR TABLE ##########')
        if check_duplicate:
            distinct = 'distinct'
        else:
            distinct = ''

        print('Checking and dropping preprocess cdr table if existing.')
        timer = time.time()
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_preprocess'.format(provider_prefix=provider_prefix))
        print('Checked and dropped preprocess cdr table if existing. Elapsed time: {} seconds'.format(
            format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating preprocess cdr table.')
        raw_sql = sql_to_string('cdr_and_mapping/create_preprocess_cdr.sql')
        query = raw_sql.format(args=', '.join(arg_cdr_prep), provider_prefix=provider_prefix)
        print(query)
        cursor.execute(query)

        print('Created preprocess cdr table. Elapsed time: {} seconds'
              .format(format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Inserting into preprocess table')
        print('Columns in preprocess table mapped: ' + ', '.join(arg_cdr_map))
        raw_sql = sql_to_string('cdr_and_mapping/insert_preprocess_cdr.sql')
        query = raw_sql.format(distinct=distinct, arg=', '.join(arg_cdr_map), provider_prefix=provider_prefix)
        print(query)
        cursor.execute(query)
        print('Inserted into preprocess cdr table. Elapsed time: {} seconds'
              .format(format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING PREPROCESS CDR TABLE ##########')

    def consolidate_table(self):
        # TODO join here
        provider_prefix = self.config.provider_prefix
        arg_cdr_prep = self.data.arg_cdr_prep
        arg_cdr_con = self.data.arg_cdr_con
        cursor = self.hc.cursor
        print('########## CREATE CONSOLIDATE CDR TABLE ##########')
        print('Checking and dropping consolidate cdr table if existing.')

        print('Checking latitude and lontitude in the preprocess table')
        cursor.execute('select max(latitude), max(longitude) from {provider_prefix}_preprocess'
                       .format(provider_prefix=provider_prefix))
        res = cursor.fetchall()

        latitude = res[0][0]
        longitude = res[0][1]
        arg_cdr_con_with_join_cond =[]
        if (latitude == -1 and longitude == -1) or True:
            print('Join to make consolidate')
            for arg in arg_cdr_con:
                if str.lower(arg) in ['longitude', 'latitude']:
                    arg_cdr_con_with_join_cond.append('a2.' + arg + ' as ' + arg)
                else:
                    arg_cdr_con_with_join_cond.append('a1.' + arg + ' as ' + arg)
            insert_script_loc = 'cdr_and_mapping/insert_consolidate_cdr_join.sql'
        else:
            arg_cdr_con_with_join_cond = arg_cdr_con
            print('No join')
            insert_script_loc = 'cdr_and_mapping/insert_consolidate_cdr.sql'

        timer = time.time()
        cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_consolidate_data_all'
                       .format(provider_prefix=provider_prefix))
        print('Checked and dropped preprocess cdr table if existing. Elapsed time: {} seconds'
              .format(format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Creating consolidate table')
        raw_sql = sql_to_string('cdr_and_mapping/create_consolidate_cdr.sql')
        query = raw_sql.format(provider_prefix=provider_prefix, arg_prep=' ,'.join(arg_cdr_prep))
        cursor.execute(query)
        print(query)
        print('Created consolidate cdr table. Elapsed time: {} seconds'
              .format(format_two_point_time(timer, time.time())))
        timer = time.time()

        print('Columns in consolidate table: ' + ', '.join(arg_cdr_con_with_join_cond))
        print('Inserting into the consolidate table')
        raw_sql = sql_to_string(insert_script_loc)
        query = raw_sql.format(provider_prefix=provider_prefix, arg_con=', '.join(arg_cdr_con_with_join_cond))
        print(query)
        cursor.execute(query)
        print('Inserted into consolidate cdr table. Elapsed time: {} seconds'
              .format(format_two_point_time(timer, time.time())))
        print('########## FINISHED CREATING CONSOLIDATE CDR TABLE ##########')

