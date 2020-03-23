from impala.dbapi import connect
from helper import json_file_to_object, get_admin_units_from_mapping
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
        if not os.path.exists(config.csv_location):
            os.makedirs(config.csv_location)
        if not os.path.exists(config.graph_location):
            os.makedirs(config.graph_location)

    def create_tables(self, config, data):
        self.import_cell_tower_data_raw(config, data)
        self.preprocess_cell_tower_data(config, data)
        #
        admins = get_admin_units_from_mapping(config.cdr_cell_tower)

        for admin in admins:
            self.cell_tower_data_admin(config, admin)

        self.import_raw(config, data)
        self.preprocess_data(config, data)
        self.consolidate_table(config, data)
        self.frequent_location(config)
        self.frequent_location_night(config)
        self.rank1_frequent_location(config)
        # pass
        # self.cdr_by_uid(config)
        # self.create_od(config)
        # self.create_od_detail()
        # self.create_od_sum()

    def import_cell_tower_data_raw(self, config, data):
        print('Checking and dropping raw mapping table if existing.')
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_raw'
                            .format(provider_prefix=config.provider_prefix))
        print('Checked and dropped raw mapping table if existing. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        print('Creating raw mapping table')
        self.cursor.execute('CREATE TABLE {provider_prefix}_cell_tower_data_raw'
                            .format(provider_prefix=config.provider_prefix) +
                            "({})".format(', '.join(data.arg_cell_raw)) +
                            'ROW FORMAT DELIMITED ' +
                            "FIELDS TERMINATED BY '{field_delimiter}' ".format(field_delimiter=config.input_cell_tower_delimiter) +
                            "LINES TERMINATED BY '\n' " +
                            "STORED AS TEXTFILE " +
                            'tblproperties ("skip.header.line.count"="1")')
        print('Created raw mapping table. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        # TODO string delimiter double quote is not checked yet (ask Ajarn.May)
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
        print('Imported to raw mapping table. Elapsed time: {} seconds'.format(time.time() - timer))

    def import_raw(self, config, data):
        print('Checking and dropping raw table if existing.')
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_raw'
                            .format(provider_prefix=config.provider_prefix))
        print('Checked and dropped raw mapping table if existing. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        print('Creating raw table')
        self.cursor.execute('CREATE TABLE {provider_prefix}_raw'
                            .format(provider_prefix=config.provider_prefix) +
                            "({})".format(', '.join(data.arg_cdr_raw)) +
                            'ROW FORMAT DELIMITED ' +
                            "FIELDS TERMINATED BY '{field_delimiter}' ".format(field_delimiter=config.input_delimiter) +
                            "LINES TERMINATED BY '\n' " +
                            "STORED AS TEXTFILE " +
                            'tblproperties ("skip.header.line.count"="{cell_tower_header}")'
                            .format(cell_tower_header=config.input_cell_tower_have_header))
        print('Created raw table. Elapsed time: {} seconds'.format(time.time() - timer))
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
        print('Imported to raw table. Elapsed time: {} seconds'.format(time.time() - timer))
        
    def cell_tower_data_admin(self, config, admin):
        if config.check_invalid_lat_lng:
            check_lat_lng = 'and (latitude != 0 or longitude != 0) and latitude is not NULL and longitude is not NULL'
        else:
            check_lat_lng = ''
        print('Checking and dropping mapping {admin} table if existing.'.format(admin=admin))
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_{admin}'.format(
            provider_prefix=config.provider_prefix, admin=admin))
        print('Check and drop mapping {admin} table if existing. Elapsed time: {time} seconds'.format(admin=admin, time=time.time() - timer))
        timer = time.time()
        print('Creating mapping {admin} table'.format(admin=admin))
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {provider_prefix}_cell_tower_data_{admin} '.format(
            provider_prefix=config.provider_prefix, admin=admin) +
                            "({admin}_id string, {admin}_name string, latitude string, longitude string)".format(admin=admin) +
                            'ROW FORMAT DELIMITED ' +
                            "FIELDS TERMINATED BY ',' " +
                            "LINES TERMINATED BY '\n' " +
                            'STORED AS SEQUENCEFILE')
        print('Created mapping {admin} table. Elapsed time: {time} seconds'.format(admin=admin, time=time.time() - timer))
        timer = time.time()

        print('Inserting into mapping {} table'.format(admin))
        insert_query = "INSERT OVERWRITE TABLE  {provider_prefix}_cell_tower_data_{admin} " \
                            "select row_number() OVER () - 1 as rowidx, {admin}, latitude, longitude  " \
                            "from {provider_prefix}_cell_tower_data_preprocess where translate({admin},'  ',' ') != '' " \
                            "{check_lat_lng} " \
                            "group by {admin}, latitude, longitude order by rowidx" \
                            .format(provider_prefix=config.provider_prefix, admin=admin, check_lat_lng=check_lat_lng)
        self.cursor.execute(insert_query)
        print('Inserted into mapping {admin} table. Elapsed time: {time} seconds'.format(admin=admin, time=time.time() - timer))

    def preprocess_cell_tower_data(self, config, data):
        if config.check_duplicate:
            distinct = 'distinct'
        else:
            distinct = ''
        print('Checking and dropping preprocess mapping table if existing.')
        timer = time.time()

        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_preprocess'.format(
            provider_prefix=config.provider_prefix))
        print('Checked and dropped preprocess mapping table if existing. Elapsed time: {} seconds'.format(
            time.time() - timer))
        timer = time.time()

        print('Creating preprocess mapping table')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {provider_prefix}_cell_tower_data_preprocess'.format(
            provider_prefix=config.provider_prefix) +
                            "({})".format(', '.join(data.arg_cell_create)) +
                            'ROW FORMAT DELIMITED ' +
                            "FIELDS TERMINATED BY ',' " +
                            "LINES TERMINATED BY '\n' " +
                            'STORED AS SEQUENCEFILE')
        print('Created mapping preprocess table. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        # need username to get privilege
        print('Inserting into preprocess mapping table')
        self.cursor.execute("INSERT INTO TABLE  {provider_prefix}_cell_tower_data_preprocess "
                            .format(provider_prefix=config.provider_prefix) +
                            "select {distinct} {arg} ".format(distinct=distinct, arg=', '.join(data.arg_cell_map)) +
                            "from {provider_prefix}_cell_tower_data_raw"
                            .format(provider_prefix=config.provider_prefix))
        print('Inserted into preprocess mapping table. Elapsed time: {} seconds'.format(time.time() - timer))

    def preprocess_data(self, config, data):
        if config.check_duplicate:
            distinct = 'distinct'
        else:
            distinct = ''

        print('Checking and dropping preprocess cdr table if existing.')
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_preprocess'.format(provider_prefix=config.provider_prefix))
        print('Checked and dropped preprocess cdr table if existing. Elapsed time: {} seconds'.format(
            time.time() - timer))
        timer = time.time()

        print('Creating preprocess cdr table.')
        create_q = 'CREATE TABLE {provider_prefix}_preprocess' \
                    "({args}) ROW FORMAT DELIMITED "\
                    "FIELDS TERMINATED BY ',' " \
                    "LINES TERMINATED BY '\n' " \
                    "STORED AS SEQUENCEFILE".format(args=', '.join(data.arg_cdr_prep), provider_prefix=config.provider_prefix)
        print('Created preprocess cdr table. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        self.cursor.execute(create_q)

        print('Inserting into preprocess table')
        print('Columns in preprocess table mapped: ' + ', '.join(data.arg_cdr_map))
        insert_q = "INSERT OVERWRITE TABLE  {provider_prefix}_preprocess " \
                   "select {distinct} {arg} from {provider_prefix}_raw "\
            .format(distinct=distinct, arg=', '.join(data.arg_cdr_map), provider_prefix=config.provider_prefix)
        self.cursor.execute(insert_q)
        print('Inserted into preprocess cdr table. Elapsed time: {} seconds'.format(time.time() - timer))

    def consolidate_table(self, config, data):
        print('Checking and dropping consolidate cdr table if existing.')
        timer = time.time()
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_consolidate_data_all'.format(
            provider_prefix=config.provider_prefix))
        print('Checked and dropped preprocess cdr table if existing. Elapsed time: {} seconds'.format(
            time.time() - timer))
        timer = time.time()
        print('Creating consolidate table')
        create_query = "CREATE TABLE {provider_prefix}_consolidate_data_all".format(
            provider_prefix=config.provider_prefix) + \
                       "({})".format(' ,'.join(data.arg_cdr_prep)) + \
                       "PARTITIONED BY (pdt string) " + \
                       "ROW FORMAT DELIMITED " + \
                       "FIELDS TERMINATED BY ','" + \
                       "LINES TERMINATED BY '\n'" + \
                       'STORED AS SEQUENCEFILE'
        print('Created consolidate cdr table. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        self.cursor.execute(create_query)
        print('Columns in consolidatie table: ' + ', '.join(data.arg_cdr_con))
        print('Inserting into the consolidate table')
        insert_query = "INSERT INTO TABLE  {provider_prefix}_consolidate_data_all ".format(
            provider_prefix=config.provider_prefix) + \
                       "PARTITION (pdt) select {}, ".format(', '.join(data.arg_cdr_con)) + \
                       "to_date(call_time) as pdt " + \
                       "from {provider_prefix}_preprocess".format(provider_prefix=config.provider_prefix)
        self.cursor.execute(insert_query)
        print('Inserted into consolidate cdr table. Elapsed time: {} seconds'.format(time.time() - timer))

    def frequent_location(self, config):
        print('Checking and dropping frequent location table if existing.')
        timer = time.time()
        admin = config.od_admin_unit
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_frequent_location'.format(provider_prefix=config.provider_prefix))
        print('Checked and dropped frequent location table if existing. Elapsed time: {} seconds'.format(
            time.time() - timer))
        timer = time.time()
        print('Creating frequent location table')
        query = "CREATE TABLE {provider_prefix}_frequent_location  (uid string, cell_id string,tcount int,trank int,ppercent double, " \
                "LONGITUDE string, LATITUDE string, {admin_params}) " \
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' " \
                "MAP KEYS TERMINATED BY '!' LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE" \
            .format(provider_prefix=config.provider_prefix, admin_params=admin + '_id string')

        self.cursor.execute(query)
        print('Created frequent location table. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        print('Inserting into frequent location table')
        query = "INSERT INTO TABLE {provider_prefix}_frequent_location SELECT a1.uid, a2.cell_id, " \
                "count(a1.uid) as tcount, ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank, " \
                "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage " \
                ", a2.longitude, a2.latitude, a3.{admin_params} from {provider_prefix}_consolidate_data_all a1 " \
                "JOIN {provider_prefix}_cell_tower_data_preprocess a2  ON(a1.cell_id = a2.cell_id) " \
                "JOIN {provider_prefix}_cell_tower_data_{admin} a3 on(a2.latitude = a3.latitude and a2.longitude = a3.longitude) " \
                "group by a1.uid, a2.latitude,  a2.longitude , a2.cell_id, a3.{admin_params} " \
                "order by a1.uid, rank ASC " \
                .format(provider_prefix=config.provider_prefix, admin_params=admin + '_id', admin=admin)

        self.cursor.execute(query)
        print('Inserted into frequent location table.\nResult are in the table named {provider_prefix}_frequent_location\nElapsed time: {time} seconds. '
              .format(provider_prefix=config.provider_prefix, time=time.time() - timer))

    def frequent_location_night(self, config):
        print('Checking and dropping frequent location night table if existing.')
        timer = time.time()
        admin = config.od_admin_unit
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_frequent_location_night'.format(provider_prefix=config.provider_prefix))
        print('Checked and dropped frequent location night table if existing. Elapsed time: {} seconds'.format(
            time.time() - timer))
        timer = time.time()
        print('Creating frequent location night table')
        query = "CREATE TABLE {provider_prefix}_frequent_location_night  (uid string, cell_id string,tcount int,trank int,ppercent double, " \
                "LONGITUDE string, LATITUDE string, {admin_params}) " \
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' " \
                "MAP KEYS TERMINATED BY '!' LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE" \
            .format(provider_prefix=config.provider_prefix, admin_params=admin + '_id string')

        self.cursor.execute(query)
        print('Created frequent location night table. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        print('Inserting into frequent location night table')
        query = "INSERT INTO TABLE {provider_prefix}_frequent_location SELECT a1.uid, a2.cell_id, " \
                "count(a1.uid) as tcount, ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank, " \
                "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage " \
                ", a2.longitude, a2.latitude, a3.{admin_params} from {provider_prefix}_consolidate_data_all a1 " \
                "JOIN {provider_prefix}_cell_tower_data_preprocess a2  ON(a1.cell_id = a2.cell_id) " \
                "JOIN {provider_prefix}_cell_tower_data_{admin} a3 on(a2.latitude = a3.latitude and a2.longitude = a3.longitude) " \
                "where hour(a1.call_time) in (0,1,2,3,4,5,6,7,20,21,22,23) group by a1.uid, a2.latitude,  a2.longitude , a2.cell_id, a3.{admin_params} " \
                "order by a1.uid, rank ASC " \
                .format(provider_prefix=config.provider_prefix, admin_params=admin + '_id', admin=admin)

        self.cursor.execute(query)
        print('Inserted into frequent location night table.\nResult are in the table named {provider_prefix}_frequent_location_night\nElapsed time: {time} seconds. '
              .format(provider_prefix=config.provider_prefix, time=time.time() - timer))

    def rank1_frequent_location(self, config):
        admin = config.od_admin_unit
        create_param = admin + '_id string'
        timer = time.time()
        print('Checking and dropping rank 1 frequent location table if existing.')
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_uid_home'.format(provider_prefix=config.provider_prefix))
        print('Checked and dropped rank 1 frequent location table if existing. Elapsed time: {} seconds'.format(
            time.time() - timer))
        timer = time.time()
        print('Creating rank 1 frequent location table')
        query = "CREATE TABLE {provider_prefix}_la_cdr_uid_home  (uid string, cell_id string, tcount " \
                "int, trank int, ppercent double, LONGITUDE string, LATITUDE string, {admin_params}) " \
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' " \
                "MAP KEYS TERMINATED BY '!' LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE " \
            .format(provider_prefix=config.provider_prefix, admin_params=create_param)
        self.cursor.execute(query)
        print('Created rank 1 frequent location table. Elapsed time: {} seconds'.format(time.time() - timer))
        timer = time.time()
        print('Inserting into rank 1 frequent location table')
        insert_q = "INSERT OVERWRITE TABLE  {provider_prefix}_la_cdr_uid_home  " \
                    "SELECT * FROM {provider_prefix}_frequent_location where trank = 1" \
                    .format(provider_prefix=config.provider_prefix)
        self.cursor.execute(insert_q)
        print('Inserted into rank 1 frequent location table. Elapsed time: {} seconds'.format(time.time() - timer))

    def cdr_by_uid(self, config):
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing.'
              .format(provider_prefix=config.provider_prefix))
        self.cursor.execute('DROP TABLE IF EXISTS la_cdr_all_with_ant_zone_by_uid')
        print('Checked and dropped {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing. Elapsed time: {time} seconds'
            .format(provider_prefix=config.provider_prefix, time=time.time() - timer))
        timer = time.time()
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table'.format(provider_prefix=config.provider_prefix))

        query = "CREATE TABLE {provider_prefix}_la_cdr_all_with_ant_zone_by_uid(uid string, arr ARRAY < ARRAY < string >>) PARTITIONED " \
                "BY(pdt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' " \
                "MAP KEYS TERMINATED BY '!' LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE".format(provider_prefix=config.provider_prefix)
        self.cursor.execute(query)

        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=time.time() - timer))
        timer = time.time()

        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table'.format(provider_prefix=config.provider_prefix))
        insert_q = "INSERT OVERWRITE TABLE  {provider_prefix}_la_cdr_all_with_ant_zone_by_uid  PARTITION (pdt)" \
                    "select a1.uid, CreateTrajectoriesJICAWithZone" \
                    "(a1.uid, call_time, call_duration, a2.longitude, a2.latitude, a1.cell_id, a2.admin1) as arr, pdt " \
                    "from {provider_prefix}_consolidate_data_all a1 join {provider_prefix}_cell_tower_data_preprocess a2 " \
                    "on (a1.cell_id = a2.cell_id) where to_date(pdt) = '2016-05-01' " \
                    "group by a1.uid, pdt " \
                    .format(provider_prefix=config.provider_prefix)

        self.cursor.execute(insert_q)
        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=time.time() - timer))

    def create_od(self, config):
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing.'
              .format(provider_prefix=config.provider_prefix))
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od'
                            .format(provider_prefix=config.provider_prefix))

        print('Checked and dropped  {provider_prefix}_la_cdr_all_with_ant_zone_by_uid table if existing. Elapsed time: {} seconds'
            .format(provider_prefix=config.provider_prefix, time=time.time() - timer))
        timer = time.time()

        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table'.format(
            provider_prefix=config.provider_prefix))
        create_q = "CREATE TABLE {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od" \
                    "(uid string,home_cell_id string,home_zone string, arr ARRAY<ARRAY<string>>)" \
                    "PARTITIONED BY (pdt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" \
                    "COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY '!'" \
                    "LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE".format(provider_prefix=config.provider_prefix)
        self.cursor.execute(create_q)

        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table'.format(
            provider_prefix=config.provider_prefix))
        timer = time.time()

        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table'.format(
            provider_prefix=config.provider_prefix))
        insert_q = "INSERT OVERWRITE TABLE {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od PARTITION (pdt) " \
                    "select t1.uid,t2.cell_id as home_cell_id, t2.{target_unit}_id as home_zone, " \
                    "TripOD(arr, t1.uid, t2.cell_id, t2.{target_unit}_id, t2.LONGITUDE, t2.LATITUDE), pdt " \
                    "from la_cdr_all_with_ant_zone_by_uid t1 inner join " \
                    "la_cdr_uid_home t2 on t1.uid = t2.uid" \
                    .format(provider_prefix=config.provider_prefix, target_unit=config.od_admin_unit)
        self.cursor.execute(insert_q)
        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=time.time() - timer))

    def create_od_detail(self, config):
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table if existing.'
              .format(provider_prefix=config.provider_prefix))
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail '
                            .format(provider_prefix=config.provider_prefix))

        print('Checked and dropped {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table if existing. Elapsed time: {} seconds'
            .format(provider_prefix=config.provider_prefix, time=time.time() - timer))
        timer = time.time()
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table'.format(
            provider_prefix=config.provider_prefix))
        create_q = "CREATE TABLE {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail " \
                   "(uid string,home_cell_id string,home_zone string, arr ARRAY<string>)" \
                    "PARTITIONED BY (pdt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" \
                    "COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY '!'" \
                    "LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE".format(provider_prefix=config.provider_prefix)
        self.cursor.execute(create_q)
        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table'.format(
            provider_prefix=config.provider_prefix))
        timer = time.time()
        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table'.format(
            provider_prefix=config.provider_prefix))

        insert_q = "INSERT OVERWRITE TABLE  {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail PARTITION (pdt) " \
                    "select uid ,home_cell_id,home_zone,m as arr,pdt " \
                    "from la_cdr_all_with_ant_zone_by_uid_od t1 " \
                   "LATERAL VIEW explode(t1.arr) myTable1 AS m".format(provider_prefix=config.provider_prefix)
        self.cursor.execute(insert_q)
        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=time.time() - timer))

    def create_od_sum(self, config):
        timer = time.time()
        print('Checking and dropping {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table if existing.'
              .format(provider_prefix=config.provider_prefix))
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum '
                            .format(provider_prefix=config.provider_prefix))
        print(
            'Checked and dropped {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table if existing. Elapsed time: {} seconds'
            .format(provider_prefix=config.provider_prefix, time=time.time() - timer))
        timer = time.time()
        print('Creating {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table'.format(
            provider_prefix=config.provider_prefix))
        create_q = "CREATE TABLE {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum " \
                   "(origin string,destination string,tcount double, tusercount double)" \
                   "PARTITIONED BY (pdt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" \
                   "COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY '!'" \
                   "LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE".format(provider_prefix=config.provider_prefix)
        self.cursor.execute(create_q)
        print('Created {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table'.format(
            provider_prefix=config.provider_prefix))
        timer = time.time()
        print('Inserting into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table'.format(
            provider_prefix=config.provider_prefix))
        insert_q = "INSERT OVERWRITE TABLE {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum PARTITION(pdt) " \
                   "select arr[2] as origin, arr[3]  as destination, count(*) as tcount, count(distinct uid) as tusercount, pdt " \
                   "from la_cdr_all_with_ant_zone_by_uid_od_detail  where ((arr[2] != '-1' and arr[3] != '-1' ) ) " \
                   "group by pdt,arr[2],arr[3]".format(provider_prefix=config.provider_prefix)
        self.cursor.execute(insert_q)
        print('Inserted into {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum table. Elapsed time: {time} seconds'
              .format(provider_prefix=config.provider_prefix, time=time.time() - timer))

        self.cursor.execute("insert overwrite local directory '/tmp/hive/csv/la_cdr_all_with_ant_zone_by_uid_od_sum.csv' select CONCAT_WS('\t',pdt,origin ,"
                            "destination,cast(tcount as string),cast(tusercount as string)) "
                            "from la_cdr_all_with_ant_zone_by_uid_od_sum t1")

        # self.cursor.execute("zip la_cdr_all_with_ant_zone_by_uid_od_sum.zip la_cdr_all_with_ant_zone_by_uid_od_sum.csv")

        #select *,sum(ppercent) over (partition by uid order by trank asc)
        # as acc_wsum from big5_frequent_location order by uid, trank limit 100;