from impala.dbapi import connect
import argparse
import json
import csv
import pandas
import os
import matplotlib.pyplot as plt
import graph_helper as hp
from matplotlib.widgets import TextBox
import os

months = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
          7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}

class CDRVisualizer:
    # TODO paramiterize the query
    def __init__(self, config_file):
        with open(config_file) as json_file:
            self.__dict__ = json.load(json_file)
        # must have username to insert to table (user privilege)
        self.conn = connect(self.host, self.port, user='rsstudent', auth_mechanism='PLAIN')
        self.cursor = self.conn.cursor()
        self.initialize()
        self.arguments_map, self.arguments_raw, self.arguments_prep, self.arguments_con \
            = self.extract_mapping_data(self.cdr_data_layer)

        self.arguments_cell_tower_data_map, self.arguments_cell_tower_data_raw, self.arguments_cell_tower_data_create, _ \
            = self.extract_mapping_data(self.cdr_cell_tower)
        self.import_cell_tower_data_raw()
        self.preprocess_cell_tower_data()
        self.import_raw()
        self.preprocess_data()
        self.calculate_data_statistics()
        self.consolidate_table()
        self.calculate_daily_statistic()
        self.calculate_monthly_statistic()
        self.calculate_frequent_locations()
        self.calculate_zone_population()
        self.calculate_user_date_histogram()
        self.calculate_summary()

    def initialize(self):
        with open('initial_hive_commands.json') as json_file:
            for command in json.load(json_file)['hive_commands']:
                self.cursor.execute(command)
        if not os.path.exists(self.csv_location):
            os.makedirs(self.csv_location)
        if not os.path.exists(self.graph_location):
            os.makedirs(self.graph_location)

    @staticmethod
    def extract_mapping_data(mapping):
        arguments_map = []
        arguments_prep = []
        arguments_raw = []
        arguments_con = []
        # Extract arguments
        for argument in mapping:
            if argument['output_no'] != -1:
                arguments_prep.append(argument['name'] + ' ' + argument['data_type'])
                arguments_con.append(argument['name'])
                if argument['input_no'] != -1:
                    arguments_raw.append(argument['input_name'] + ' ' + argument['data_type'])

                    # TODO validate each field in the custom arguments and make sure their input_no is not -1
                    if 'custom' in argument:
                        arguments_map.append(argument['custom'] + ' as ' + argument['name'])
                    else:
                        arguments_map.append(argument['input_name'] + ' as ' + argument['name'])
                else:
                    # TODO validate each field in the custom arguments and make sure their input_no is not -1
                    if 'custom' in argument:
                        arguments_map.append(argument['custom'] + ' as ' + argument['name'])
                    else:
                        arguments_map.append('-1' + ' as ' + argument['name'])
            elif argument['input_no'] != -1:
                arguments_raw.append(argument['input_name'] + ' ' + argument['data_type'])

        return arguments_map, arguments_raw, arguments_prep, arguments_con

    def import_cell_tower_data_raw(self):
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_raw'
                            .format(provider_prefix=self.provider_prefix))

        self.cursor.execute('CREATE TABLE {provider_prefix}_cell_tower_data_raw'
                            .format(provider_prefix=self.provider_prefix) +
                            "({})".format(', '.join(self.arguments_cell_tower_data_raw)) +
                            'ROW FORMAT DELIMITED ' +
                            "FIELDS TERMINATED BY ',' " +
                            "LINES TERMINATED BY '\n' " +
                            "STORED AS TEXTFILE " +
                            'tblproperties ("skip.header.line.count"="1")')

        # TODO string delimiter double quote is not checked yet (ask Ajarn.May)
        if len(self.input_cell_tower_files) < 1:
            print('Please check the input_cell_tower_files field in config.json and make sure the file is valid.')
            return
        elif len(self.input_cell_tower_files) == 1:
            self.cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=self.hadoop_data_path, hadoop_data_file=self.input_cell_tower_files[0]) +
                "overwrite into table {provider_prefix}_cell_tower_data_raw".format(
                    provider_prefix=self.provider_prefix)
            )
        else:
            self.cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=self.hadoop_data_path, hadoop_data_file=self.input_cell_tower_files[0]) +
                "overwrite into table {provider_prefix}_cell_tower_data_raw".format(
                    provider_prefix=self.provider_prefix)
            )
            for i in range(1, len(self.input_cell_tower_files)):
                self.cursor.execute(
                    "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                    .format(hadoop_data_path=self.hadoop_data_path, hadoop_data_file=self.input_cell_tower_files[i]) +
                    "into table {provider_prefix}_cell_tower_data_raw".format(provider_prefix=self.provider_prefix)
                )

    def import_raw(self):
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_raw'
                            .format(provider_prefix=self.provider_prefix))
        self.cursor.execute('CREATE TABLE {provider_prefix}_raw'
                            .format(provider_prefix=self.provider_prefix) +
                            "({})".format(', '.join(self.arguments_raw)) +
                            'ROW FORMAT DELIMITED ' +
                            "FIELDS TERMINATED BY ',' " +
                            "LINES TERMINATED BY '\n' " +
                            "STORED AS TEXTFILE " +
                            'tblproperties ("skip.header.line.count"="1")')
        # TODO string delimiter double quote is not checked yet (ask Ajarn.May)
        if len(self.input_files) < 1:
            'Please check the input_files field in config.json and make sure the file is valid.'
            return
        elif len(self.input_files) == 1:
            self.cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=self.hadoop_data_path, hadoop_data_file=self.input_files[0]) +
                "overwrite into table {provider_prefix}_raw".format(provider_prefix=self.provider_prefix)
            )
        else:
            self.cursor.execute(
                "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                .format(hadoop_data_path=self.hadoop_data_path, hadoop_data_file=self.input_files[0]) +
                "overwrite into table {provider_prefix}_raw".format(provider_prefix=self.provider_prefix)
            )
            for i in range(1, len(self.input_files)):
                self.cursor.execute(
                    "load data local inpath '{hadoop_data_path}{hadoop_data_file}' "
                    .format(hadoop_data_path=self.hadoop_data_path, hadoop_data_file=self.input_files[i]) +
                    "into table {provider_prefix}_raw".format(provider_prefix=self.provider_prefix)
                )

    def preprocess_cell_tower_data(self):
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_preprocess'.format(
            provider_prefix=self.provider_prefix))
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {provider_prefix}_cell_tower_data_preprocess'.format(
            provider_prefix=self.provider_prefix) +
                            "({})".format(', '.join(self.arguments_cell_tower_data_create)) +
                            'ROW FORMAT DELIMITED ' +
                            "FIELDS TERMINATED BY ',' " +
                            "LINES TERMINATED BY '\n' " +
                            'STORED AS SEQUENCEFILE')

        # delete_query = "delete from {provider_prefix}_cell_tower_data_preprocess".format(
        #     provider_prefix=self.provider_prefix)
        # self.cursor.execute(delete_query)
        # need username to get privilege
        print('### Inserting into cell tower data preprocessing table ###')
        self.cursor.execute("INSERT INTO TABLE  {provider_prefix}_cell_tower_data_preprocess "
                            .format(provider_prefix=self.provider_prefix) +
                            "select {} ".format(', '.join(self.arguments_cell_tower_data_map)) +
                            "from {provider_prefix}_cell_tower_data_raw"
                            .format(provider_prefix=self.provider_prefix))

    def preprocess_data(self):
        print('### Creating preprocessing table if not existing ###')
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_preprocess'.format(provider_prefix=self.provider_prefix))
        self.cursor.execute(
            'CREATE TABLE {provider_prefix}_preprocess'.format(provider_prefix=self.provider_prefix) +
            "({})".format(', '.join(self.arguments_prep)) +
            'ROW FORMAT DELIMITED ' +
            "FIELDS TERMINATED BY ',' " +
            "LINES TERMINATED BY '\n' " +
            "STORED AS SEQUENCEFILE")

        # need username to get privilege
        print('### Inserting into preprocessing  table ###')
        self.cursor.execute("INSERT OVERWRITE TABLE  {provider_prefix}_preprocess "
                            .format(provider_prefix=self.provider_prefix) +
                            "select {} ".format(', '.join(self.arguments_map)) +
                            "from {provider_prefix}_raw"
                            .format(provider_prefix=self.provider_prefix))

    def calculate_data_statistics(self):
        query = "select count(*) as total_records, " + \
                "count(distinct from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd')) as total_days, " + \
                "count(distinct uid) as unique_id, " + \
                "count(distinct IMEI) as unique_imei, " + \
                "count(distinct IMSI) as unique_imsi, " + \
                "count(distinct cell_id) as unique_location_name, " + \
                "from_unixtime(unix_timestamp(min(call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss') as start_date, " + \
                "from_unixtime(unix_timestamp(max(call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')  as end_date " + \
                "from {provider_prefix}_preprocess ".format(provider_prefix=self.provider_prefix)

        print('### Calculating data statistics ###')
        self.cursor.execute(query)

        # TODO where to store? in the vm server or in the local machine
        # TODO try in the local machine

        with open("{}/css_file_data_stat.csv".format(self.csv_location), "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in self.cursor.description)
            for row in self.cursor:
                writer.writerow(row)
        print('### Successfully wrote to css_file_data_stat.csv ###')

    def consolidate_table(self):

        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_consolidate_data_all'.format(
            provider_prefix=self.provider_prefix))

        create_query = "CREATE TABLE {provider_prefix}_consolidate_data_all".format(
            provider_prefix=self.provider_prefix) + \
                       "({})".format(' ,'.join(self.arguments_prep)) + \
                       "PARTITIONED BY (pdt string) " + \
                       "ROW FORMAT DELIMITED " + \
                       "FIELDS TERMINATED BY ','" + \
                       "LINES TERMINATED BY '\n'" + \
                       'STORED AS SEQUENCEFILE'
        self.cursor.execute(create_query)
        print('### Inserting into the consolidate table ###')
        insert_query = "INSERT INTO TABLE  {provider_prefix}_consolidate_data_all ".format(
            provider_prefix=self.provider_prefix) + \
                       "PARTITION (pdt) select {}, ".format(', '.join(self.arguments_con)) + \
                       "(call_time) as pdt " + \
                       "from {provider_prefix}_preprocess".format(provider_prefix=self.provider_prefix)
        self.cursor.execute(insert_query)

    def calculate_daily_statistic(self):
        results = []
        start_date = ''
        end_date = ''
        with open('{}/css_file_data_stat.csv'.format(self.csv_location)) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 1:
                    start_date = row[6]
                    end_date = row[7]
                    break
                line_count += 1

        start_date = (str(pandas.Timestamp(start_date))).split(' ')[0]
        end_date = (str(pandas.Timestamp(end_date))).split(' ')[0]
        print(start_date, end_date)
        print('### Calculating Daily Statistics ###')
        # FOR CASE ALL
        query = "SELECT to_date(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as date, 'ALL' as call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                "COUNT(DISTINCT TO_DATE(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))) as total_days, " + \
                'COUNT(DISTINCT uid) as unique_id, ' + \
                'COUNT(DISTINCT imei) as unique_imei, COUNT(DISTINCT imsi) unique_imsi, ' + \
                'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                "FROM {provider_prefix}_consolidate_data_all where to_date(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between to_date('{start_date}') and to_date('{end_date}') " \
                    .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                "GROUP BY to_date(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))"

        query += ' UNION '

        query += "SELECT to_date(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as date, call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                 "COUNT(DISTINCT TO_DATE(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))) as total_days, " + \
                 'COUNT(DISTINCT uid) as unique_id, ' + \
                 'COUNT(DISTINCT imei) as unique_imei, COUNT(DISTINCT imsi) unique_imsi, ' + \
                 'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                 "FROM {provider_prefix}_consolidate_data_all where to_date(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between to_date('{start_date}') and to_date('{end_date}') " \
                     .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                 "GROUP BY to_date(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), call_type"

        query += ' UNION '

        query += "SELECT to_date(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as date, 'ALL' as call_type,  network_type, COUNT(*) as total_records, " + \
                 "COUNT(DISTINCT TO_DATE(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))) as total_days, " + \
                 'COUNT(DISTINCT uid) as unique_id, ' + \
                 'COUNT(DISTINCT imei) as unique_imei, COUNT(DISTINCT imsi) unique_imsi, ' + \
                 'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                 "FROM {provider_prefix}_consolidate_data_all where to_date(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between to_date('{start_date}') and to_date('{end_date}') " \
                     .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                 "GROUP BY to_date(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), network_type"

        query += ' UNION '

        query += "SELECT to_date(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as date, call_type, network_type, COUNT(*) as total_records, " + \
                 "COUNT(DISTINCT TO_DATE(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))) as total_days, " + \
                 'COUNT(DISTINCT uid) as unique_id, ' + \
                 'COUNT(DISTINCT imei) as unique_imei, COUNT(DISTINCT imsi) unique_imsi, ' + \
                 'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                 "FROM {provider_prefix}_consolidate_data_all where to_date(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between to_date('{start_date}') and to_date('{end_date}') " \
                     .format(provider_prefix=self.provider_prefix, start_date=start_date, end_date=end_date) + \
                 "GROUP BY to_date(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), call_type, network_type ORDER BY date ASC, call_type ASC, network_type DESC"

        self.cursor.execute(query)
        description = self.cursor.description
        results += self.cursor.fetchall()
        file_path = '{}/css_provider_data_stat_daily.csv'.format(self.csv_location)
        if os.path.exists(file_path):
            os.remove(file_path)

        with open(file_path, "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in description)
            for row in results:
                writer.writerow(row)

        print('### Successfully wrote to file css_provider_data_stat_daily.csv ###')
        # TODO output to a graph

    def calculate_monthly_statistic(self):
        results = []
        start_date = ''
        end_date = ''
        with open('{}/css_file_data_stat.csv'.format(self.csv_location)) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 1:
                    start_date = row[6]
                    end_date = row[7]
                    break
                line_count += 1

        start_date = pandas.Timestamp(start_date)
        start_m = start_date.month
        start_y = start_date.year
        end_date = pandas.Timestamp(end_date)
        end_m = end_date.month
        end_y = end_date.year
        print(start_date, start_m, start_y, end_date, end_m, end_y)
        print('### Calculating Monthly Statistics ###')
        # FOR CASE ALL
        query = "SELECT YEAR(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as year, MONTH(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as month  , 'ALL' as call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                "COUNT(DISTINCT TO_DATE(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))) as total_days, " + \
                'COUNT(DISTINCT uid) as unique_id, ' + \
                'COUNT(DISTINCT imei) as unique_imei, COUNT(DISTINCT imsi) unique_imsi, ' + \
                'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                "FROM {provider_prefix}_consolidate_data_all where (year(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between {start_year} and {end_year}) " \
                    .format(provider_prefix=self.provider_prefix, start_year=start_y, end_year=end_y) + \
                "and (MONTH(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between {start_month} and {end_month}) GROUP BY YEAR(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), MONTH(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))" \
                    .format(start_month=start_m, end_month=end_m)

        query += ' UNION '

        query += "SELECT YEAR(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as year, MONTH(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as month, call_type, 'ALL' as network_type, COUNT(*) as total_records, " + \
                 "COUNT(DISTINCT TO_DATE(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))) as total_days, " + \
                 'COUNT(DISTINCT uid) as unique_id, ' + \
                 'COUNT(DISTINCT imei) as unique_imei, COUNT(DISTINCT imsi) unique_imsi, ' + \
                 'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                 "FROM {provider_prefix}_consolidate_data_all where (year(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between {start_year} and {end_year}) " \
                     .format(provider_prefix=self.provider_prefix, start_year=start_y, end_year=end_y) + \
                 "and (MONTH(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between {start_month} and {end_month}) GROUP BY YEAR(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), MONTH(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), call_type" \
                     .format(start_month=start_m, end_month=end_m)

        query += ' UNION '

        query += "SELECT YEAR(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as year, MONTH(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as month, 'ALL' as call_type,  network_type, COUNT(*) as total_records, " + \
                 "COUNT(DISTINCT TO_DATE(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))) as total_days, " + \
                 'COUNT(DISTINCT uid) as unique_id, ' + \
                 'COUNT(DISTINCT imei) as unique_imei, COUNT(DISTINCT imsi) unique_imsi, ' + \
                 'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                 "FROM {provider_prefix}_consolidate_data_all where (year(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between {start_year} and {end_year}) " \
                     .format(provider_prefix=self.provider_prefix, start_year=start_y, end_year=end_y) + \
                 "and (MONTH(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between {start_month} and {end_month}) GROUP BY YEAR(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), MONTH(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), network_type" \
                     .format(start_month=start_m, end_month=end_m)

        query += ' UNION '
        query += "SELECT YEAR(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as year, MONTH(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) as month , call_type, network_type, COUNT(*) as total_records, " + \
                 "COUNT(DISTINCT TO_DATE(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'))) as total_days, " + \
                 'COUNT(DISTINCT uid) as unique_id, ' + \
                 'COUNT(DISTINCT imei) as unique_imei, COUNT(DISTINCT imsi) unique_imsi, ' + \
                 'COUNT(DISTINCT cell_id) as unique_location_name ' + \
                 "FROM {provider_prefix}_consolidate_data_all where (year(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between {start_year} and {end_year}) " \
                     .format(provider_prefix=self.provider_prefix, start_year=start_y, end_year=end_y) + \
                 "and (MONTH(from_unixtime(unix_timestamp((pdt) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')) between {start_month} and {end_month}) GROUP BY YEAR(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), MONTH(from_unixtime(unix_timestamp((call_time) ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss')), call_type, network_type ".format(
                     start_month=start_m, end_month=end_m) + \
                 "ORDER BY year ASC, month ASC, call_type ASC, network_type DESC"

        self.cursor.execute(query)
        description = self.cursor.description
        results += self.cursor.fetchall()

        file_path = '{}/css_provider_data_stat_monthly.csv'.format(self.csv_location)
        if os.path.exists(file_path):
            os.remove(file_path)

        with open(file_path, "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in description)
            for row in results:
                writer.writerow(row)

        print('### Successfully wrote to file css_provider_data_stat_monthly.csv ###')
        # TODO output to a graph

    def calculate_frequent_locations(self):
        has_cell_id = False
        for col in self.cdr_cell_tower:
            if col['name'] == 'CELL_ID' and 'output_no' != -1:
                has_cell_id = True
                break

        if (has_cell_id):  # join by cell_id and get its admin unit
            query = "select a1.uid, count(a1.uid) as count, " + \
                    "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage, " + \
                    "ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank" + \
                    ", concat(a2.latitude, ' : ', a2.longitude) as unique_location " + \
                    "from {provider_prefix}_consolidate_data_all a1 ".format(provider_prefix=self.provider_prefix) + \
                    "JOIN {provider_prefix}_cell_tower_data_preprocess a2 ".format(
                        provider_prefix=self.provider_prefix) + \
                    "ON(a1.cell_id = a2.cell_id) group by a1.uid, " + \
                    "concat(a2.latitude, ' : ', a2.longitude) " + \
                    "order by a1.uid, count DESC "

        else:  # join by lat_lng
            query = "select a1.uid, count(a1.uid) as count, " + \
                    "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage, " + \
                    "ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank " + \
                    ", concat(a1.latitude, ' : ', a1.longitude) as unique_location " + \
                    "from {provider_prefix}_consolidate_data_all a1 group by a1.uid, concat(a1.latitude, ' : ', a1.longitude)".format(
                        provider_prefix=self.provider_prefix) + \
                    "order by uid, count DESC"

        self.cursor.execute(query)
        accumulate = 0
        active_id = 0
        row_i = 1
        description = self.cursor.description
        rows = self.cursor.fetchall()
        while row_i < len(rows):
            if rows[row_i][0] == active_id:
                if accumulate < self.frequent_location_percentage:
                    accumulate += rows[row_i][2]
                    row_i += 1
                else:
                    if rows[row_i][2] != rows[row_i - 1]:
                        del rows[row_i]
                    else:
                        row_i += 1
            else:
                accumulate = rows[row_i][2]
                active_id = rows[row_i][0]
                row_i += 1

        file_path = '{}/frequent_locations.csv'.format(self.csv_location)
        if os.path.exists(file_path):
            os.remove(file_path)

        with open(file_path, "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in description)
            for row in rows:
                writer.writerow(row)

    def calculate_frequent_locations_night(self):
        has_cell_id = False
        for col in self.cdr_cell_tower:
            if col['name'] == 'CELL_ID' and 'output_no' != -1:
                has_cell_id = True
                break

        if (has_cell_id):  # join by cell_id and get its admin unit
            query = "select a1.uid, count(a1.uid) as count, " + \
                    "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage, " + \
                    "ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank" + \
                    ", concat(a2.latitude, ' : ', a2.longitude) as unique_location " + \
                    "from {provider_prefix}_consolidate_data_all a1 ".format(provider_prefix=self.provider_prefix) + \
                    "JOIN {provider_prefix}_cell_tower_data_preprocess a2 ".format(
                        provider_prefix=self.provider_prefix) + \
                    "ON(a1.cell_id = a2.cell_id) group by a1.uid, " + \
                    "concat(a2.latitude, ' : ', a2.longitude) " + \
                    "order by a1.uid, count DESC "

        else:  # join by lat_lng
            query = "select a1.uid, count(a1.uid) as count, " + \
                    "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage, " + \
                    "ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank " + \
                    ", concat(a1.latitude, ' : ', a1.longitude) as unique_location " + \
                    "from {provider_prefix}_consolidate_data_all a1 group by a1.uid, concat(a1.latitude, ' : ', a1.longitude)".format(
                        provider_prefix=self.provider_prefix) + \
                    "order by uid, count DESC"

        self.cursor.execute(query)
        accumulate = 0
        active_id = 0
        row_i = 1
        description = self.cursor.description
        rows = self.cursor.fetchall()
        while row_i < len(rows):
            if rows[row_i][0] == active_id:
                if accumulate < self.frequent_location_percentage:
                    accumulate += rows[row_i][2]
                    row_i += 1
                else:
                    if rows[row_i][2] != rows[row_i - 1]:
                        del rows[row_i]
                    else:
                        row_i += 1
            else:
                accumulate = rows[row_i][2]
                active_id = rows[row_i][0]
                row_i += 1

        file_path = '{}/frequent_locations.csv'.format(self.csv_location)
        if os.path.exists(file_path):
            os.remove(file_path)

        with open(file_path, "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in description)
            for row in rows:
                writer.writerow(row)

    def calculate_frequent_locations_day(self):
        has_cell_id = False
        for col in self.cdr_cell_tower:
            if col['name'] == 'CELL_ID' and 'output_no' != -1:
                has_cell_id = True
                break

        if (has_cell_id):  # join by cell_id and get its admin unit
            query = "select a1.uid, count(a1.uid) as count, " + \
                    "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage, " + \
                    "ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank" + \
                    ", concat(a2.latitude, ' : ', a2.longitude) as unique_location " + \
                    "from {provider_prefix}_consolidate_data_all a1 ".format(provider_prefix=self.provider_prefix) + \
                    "JOIN {provider_prefix}_cell_tower_data_preprocess a2 ".format(
                        provider_prefix=self.provider_prefix) + \
                    "ON(a1.cell_id = a2.cell_id) group by a1.uid, " + \
                    "concat(a2.latitude, ' : ', a2.longitude) " + \
                    "order by a1.uid, count DESC "

        else:  # join by lat_lng
            query = "select a1.uid, count(a1.uid) as count, " + \
                    "count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as percentage, " + \
                    "ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank " + \
                    ", concat(a1.latitude, ' : ', a1.longitude) as unique_location " + \
                    "from {provider_prefix}_consolidate_data_all a1 group by a1.uid, concat(a1.latitude, ' : ', a1.longitude)".format(
                        provider_prefix=self.provider_prefix) + \
                    "order by uid, count DESC"

        self.cursor.execute(query)
        accumulate = 0
        active_id = 0
        row_i = 1
        description = self.cursor.description
        rows = self.cursor.fetchall()
        while row_i < len(rows):
            if rows[row_i][0] == active_id:
                if accumulate < self.frequent_location_percentage:
                    accumulate += rows[row_i][2]
                    row_i += 1
                else:
                    if rows[row_i][2] != rows[row_i - 1]:
                        del rows[row_i]
                    else:
                        row_i += 1
            else:
                accumulate = rows[row_i][2]
                active_id = rows[row_i][0]
                row_i += 1

        file_path = '{}/frequent_locations.csv'.format(self.csv_location)
        if os.path.exists(file_path):
            os.remove(file_path)

        with open(file_path, "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in description)
            for row in rows:
                writer.writerow(row)

    def calculate_zone_population(self):
        admin_units = ['ADMIN0', 'ADMIN1', 'ADMIN2', 'ADMIN3', 'ADMIN4', 'ADMIN5']
        admin_units_active = []
        geo_jsons_active = []
        name_columns = []
        has_cell_id = False
        for col in self.cdr_cell_tower:
            if col['name'] == 'CELL_ID' and 'output_no' != -1:
                has_cell_id = True
            if col['name'] in admin_units:
                admin_units_active.append(col['name'])
                with open(col['geojson_filename'], encoding="utf-8") as json_file:
                    geo_jsons_active.append(json.load(json_file))
                name_columns.append(col['geojson_col_name'])
        geo_i = 0
        for admin_unit in admin_units_active:
            if has_cell_id:
                query = (
                            "select lv as {level}, sum(td.count) as count_activities, count(td.uid) as count_unique_ids from "
                            "(select a1.{level} as lv, " +
                            "count(a1.{level}) as count, a2.uid as uid " +
                            "from {provider_prefix}_cell_tower_data_preprocess a1 ".format(
                                provider_prefix=self.provider_prefix) +
                            "JOIN {provider_prefix}_consolidate_data_all a2 ".format(
                                provider_prefix=self.provider_prefix) +
                            "on (a1.cell_id = a2.cell_id) " +
                            "group by a1.{level}, a2.uid) td group by lv").format(level=admin_unit)
            else:
                query = (
                            "select lv1 as {level}, sum(td2.count) as count_activities, count(td2.uid) as count_unique_ids from"
                            "("
                            "select lv as lv1, a2.uid as uid, count(*) as count from " +
                            "("
                            "select distinct a1.latitude, a1.longitude, {level} as lv from {provider_prefix}_cell_tower_data_preprocess a1) td ".format(
                                provider_prefix=self.provider_prefix) +
                            "JOIN {provider_prefix}_consolidate_data_all a2 on (td.latitude = a2.latitude and td.longitude = a2.longitude) group by lv, a2.uid) td2 ".format(
                                provider_prefix=self.provider_prefix) +
                            "group by lv1"
                            ).format(level=admin_unit)

            self.cursor.execute(query)
            description = self.cursor.description
            rows = self.cursor.fetchall()
            file_path = '{csv_location}/zone_based_aggregations_level_{level}.csv'.format(csv_location=self.csv_location, level=admin_unit)
            for f in range(0, len(geo_jsons_active[geo_i]['features'])):

                if geo_jsons_active[geo_i]['features'][f]['properties'][name_columns[geo_i]] == 'Kochi Ken':
                    geo_jsons_active[geo_i]['features'][f]['properties']['num_population'] = 'Kochi Ken'

            if os.path.exists(file_path):
                os.remove(file_path)

            with open(file_path, "w", newline='') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(col[0] for col in description)
                for row in rows:
                    writer.writerow(row)

            with open(col['geojson_filename'][:-4] + '_joined_' + admin_unit + '.json', "w", newline='') as outfile:
                json.dump(geo_jsons_active[geo_i], outfile)
            geo_i += 1

    def calculate_user_date_histogram(self):

        query = ("select explode(histogram_numeric(active_days, 10)) as active_day_bins from "
                 "(select count(*) as active_days, td.uid from "
                 "(select year(to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd'))) as year, "
                 "month(to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd'))) as month, "
                 "day(to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd'))) as day, uid "
                 "from {provider_prefix}_consolidate_data_all group by uid, year(to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd'))), "
                 "month(to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd'))), "
                 "day(to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd'))) order by year, month, day, uid) td "
                 "group by td.uid) td2".format(provider_prefix=self.provider_prefix))

        self.cursor.execute(query)
        description = self.cursor.description
        rows = self.cursor.fetchall()
        file_path = '{}/histogram.csv'.format(self.csv_location)
        if os.path.exists(file_path):
            os.remove(file_path)
        print(rows)
        xs = []
        ys = []
        for row in rows:
            json_data = json.loads(row[0])
            xs.append(json_data['x'])
            ys.append(json_data['y'])

        plt.bar(xs, ys, align='center')  # A bar chart
        plt.xlabel('Active Day Bins')
        plt.ylabel('Count No. Unique Ids')
        plt.savefig('{}/user_data_histogram.png'.format(self.graph_location))

        with open(file_path, "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in description)
            for row in rows:
                writer.writerow(row)

    def calculate_summary(self):
        tb_1_description = ('All Data', 'Value')
        tb_2_description = ('Statistics',)
        output_1_rows = []
        print('Calculating total records')
        q_total_records = 'select count(*) as total_records from {provider_prefix}_consolidate_data_all'.format(
            provider_prefix=self.provider_prefix)
        self.cursor.execute(q_total_records)
        des = self.cursor.description
        row_total_records = self.cursor.fetchall()
        row_total_records = (des[0][0], row_total_records[0][0])
        print(row_total_records)
        output_1_rows.append(row_total_records)
        total_records = row_total_records[1]
        print('Successfully calculated total records')

        print('Calculating total unique uids')
        q_total_uids = 'select count(*) as total_uids from (select distinct uid from {provider_prefix}_consolidate_data_all) td'.format(
            provider_prefix=self.provider_prefix)
        self.cursor.execute(q_total_uids)
        des = self.cursor.description
        row_total_uids = self.cursor.fetchall()
        row_total_uids = (des[0][0], row_total_uids[0][0])
        print(row_total_uids)
        output_1_rows.append(row_total_uids)
        total_uids = row_total_uids[1]
        print('Successfully calculated total unique uids')

        print('Calculating total days')
        q_total_days = " select count(*) as total_days, min(dates) as start_date, max(dates) as end_date from (select  to_date( " \
                       " from_unixtime( unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' )) as dates " \
                       "from dtac_consolidate_data_all " \
                       "group by to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' ))) td" \
            .format(provider_prefix=self.provider_prefix)


        self.cursor.execute(q_total_days)
        des = self.cursor.description
        row_total_days = self.cursor.fetchall()

        total_days = row_total_days[0][0]
        start_yyyy_mm_dd = row_total_days[0][1].split('-')
        end_yyyy_mm_dd = row_total_days[0][2].split('-')

        start_day = start_yyyy_mm_dd[2]
        start_month = start_yyyy_mm_dd[1]
        start_year = start_yyyy_mm_dd[0]

        end_day = end_yyyy_mm_dd[2]
        end_month = end_yyyy_mm_dd[1]
        end_year = end_yyyy_mm_dd[0]

        if int(total_days) == 0:
            row_total_days = (des[0][0], row_total_days[0][0])
        elif int(total_days) == 1:
            row_total_days = (des[0][0],
                              str(row_total_days[0][0]) + ' ({} {} {})'.format(int(start_day), months[int(start_month)],
                                                                               start_year))
        elif int(total_days) >= 2:
            if start_year == end_year:
                # same year
                if start_month == end_month:
                    # no same day because it is gonna be total_days 1, which is done above
                    row_total_days = (des[0][0],
                                      str(row_total_days[0][0]) + ' ({}-{} {} {})'.format(int(start_day), end_day,
                                                                                          months[int(start_month)],
                                                                                          start_year))
                else:
                    # for different months, same or different day will also be outputted
                    row_total_days = (des[0][0], str(row_total_days[0][0]) + ' ({} {}-{} {} {})'.format(int(start_day),
                                                                                                        months[int(
                                                                                                            start_month)],
                                                                                                        int(end_day),
                                                                                                        months[int(
                                                                                                            end_month)],
                                                                                                        start_year))

            else:
                # for the more-than-one-year case, everything is displayed
                row_total_days = (des[0][0], str(row_total_days[0][0]) + ' ({} {} {}-{} {} {})'.format(int(start_day),
                                                                                                       months[int(
                                                                                                           start_month)],
                                                                                                       start_year,
                                                                                                       int(end_day),
                                                                                                       months[int(
                                                                                                           end_month)],
                                                                                                       end_year))

        print(row_total_days)
        output_1_rows.append(row_total_days)
        print('Successfully calculated total days')

        print('Calculating daily average location name')
        has_cell_id = False
        for col in self.cdr_cell_tower:
            if col['name'] == 'CELL_ID' and 'output_no' != -1:
                has_cell_id = True
                break
        if has_cell_id:
            q_total_locations = "select count(*) as count_unique_locations from (select distinct a2.latitude, a2.longitude from dtac_consolidate_data_all a1 " \
                                "join dtac_cell_tower_data_preprocess a2 " \
                                "on(a1.cell_id = a2.cell_id)) td".format(provider_prefix=self.provider_prefix)
        else:
            q_total_locations = 'select count(*) as total_unique_locations from (select latitude, longitude from {provider_prefix}_consolidate_data_all group by latitude, longitude) td'.format(
                provider_prefix=self.provider_prefix)
        self.cursor.execute(q_total_locations)
        des = self.cursor.description
        row_total_locations = self.cursor.fetchall()
        row_total_locations = (des[0][0], row_total_locations[0][0])
        print(row_total_locations)
        output_1_rows.append(row_total_locations)
        total_unique_locations = row_total_locations[1]
        print('Successfully calculated daily average location name')

        # average usage per day
        print('Calculating average daily usage')
        output_2_rows = []
        row_avg_daily_usage = ('average_usage_per_day', float(total_records / total_days))
        print(row_avg_daily_usage)
        output_2_rows.append(row_avg_daily_usage)
        print('Successfully calculated average daily usage')
        # avg voice call per day
        print('Calculating average daily voice call usage')
        q_avg_daily_voice = "select count(*)/{total_records} as average_daily_voice from {provider_prefix}_consolidate_data_all where call_type = 'VOICE'".format(
            provider_prefix=self.provider_prefix, total_records=total_records)
        self.cursor.execute(q_avg_daily_voice)
        des = self.cursor.description
        row_avg_daily_voice = self.cursor.fetchall()
        row_avg_daily_voice = (des[0][0], row_avg_daily_voice[0][0])
        print(row_avg_daily_voice)
        output_2_rows.append(row_avg_daily_voice)
        print('Successfully calculated average daily voice call usage')
        # avg sms per day
        print('Calculating average daily sms usage')
        q_avg_daily_sms = "select count(*)/{total_records} as average_daily_sms from {provider_prefix}_consolidate_data_all where call_type = 'SMS'".format(
            provider_prefix=self.provider_prefix, total_records=total_records)
        self.cursor.execute(q_avg_daily_sms)
        des = self.cursor.description
        row_avg_daily_sms = self.cursor.fetchall()
        row_avg_daily_sms = (des[0][0], row_avg_daily_sms[0][0])
        print(row_avg_daily_sms)
        output_2_rows.append(row_avg_daily_sms)
        print('Successfully calculated average daily sms usage')
        # avg unique cell id
        print('Calculating average daily unique cell id')
        q_avg_daily_unique_cell_id = "select count(*)/{total_records} as average_daily_unique_cell_id from (select distinct cell_id from dtac_consolidate_data_all) td" \
            .format(provider_prefix=self.provider_prefix, total_records=total_records)

        self.cursor.execute(q_avg_daily_unique_cell_id)
        des = self.cursor.description
        row_avg_daily_unique_cell_id = self.cursor.fetchall()
        row_avg_daily_unique_cell_id = (des[0][0], row_avg_daily_unique_cell_id[0][0])
        print(row_avg_daily_unique_cell_id)
        output_2_rows.append(row_avg_daily_unique_cell_id)
        print('Successfully calculated average daily unique cell id')
        # avg district
        print('Calculating average daily district')
        q_avg_daily_district = (
                "select count(distinct a1.{level})/{total_records} as average_district_per_day " \
                "from {provider_prefix}_cell_tower_data_preprocess a1 " \
                "JOIN {provider_prefix}_consolidate_data_all a2 " \
                "on (a1.cell_id = a2.cell_id)")\
            .format(provider_prefix=self.provider_prefix, level='ADMIN1', total_records=total_records)

        self.cursor.execute(q_avg_daily_district)
        des = self.cursor.description
        row_avg_daily_district = self.cursor.fetchall()
        row_avg_daily_district = (des[0][0], row_avg_daily_district[0][0])
        print(row_avg_daily_district)
        output_2_rows.append(row_avg_daily_district)
        print('Successfully Calculated average daily district')
        print('Recording to summary_stats')

        with open("{}/summary_stats.csv".format(self.csv_location), "w", newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(tb_1_description)
            for row in output_1_rows:
                writer.writerow(row)

            writer.writerow('\n')

            writer.writerow(tb_2_description)
            for row in output_2_rows:
                writer.writerow(row)

        print('### Successfully wrote to summary_stats.csv ###')

        q_total_daily_cdr = "select to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' )) as date, " \
                            "count(*) as total_records from {provider_prefix}_consolidate_data_all group by " \
                            "to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' ))".format(provider_prefix=self.provider_prefix)
        self.cursor.execute(q_total_daily_cdr)
        row_total_daily_cdr = self.cursor.fetchall()
        total_daily_cdr_x = []
        total_daily_cdr_y = []
        for row in row_total_daily_cdr:
            total_daily_cdr_x.append(row[0])
            total_daily_cdr_y.append(row[1])

        q_total_daily_cdr_all = "select min(total_records), max(total_records), avg(total_records) from ({}) td".format(q_total_daily_cdr)
        self.cursor.execute(q_total_daily_cdr_all)

        row_total_daily_cdr_all = self.cursor.fetchall()
        daily_cdr_min, daily_cdr_max, daily_cdr_avg = row_total_daily_cdr_all[0][0], row_total_daily_cdr_all[0][1], row_total_daily_cdr_all[0][2]
        print(row_total_daily_cdr)
        hp.make_graph(total_daily_cdr_x, 'Day', total_daily_cdr_y, 'Total Records', 'Daily CDRs', '{}/daily_cdrs'.format(self.graph_location),
                      des_pair_1={'text_x': 0.075, 'text_y': 1.27, 'text': 'MIN', 'value': f"{daily_cdr_min:,.2f}"},
                      des_pair_2={'text_x': 0.33, 'text_y': 1.27, 'text': 'MAX', 'value': f"{daily_cdr_max:,.2f}"},
                      des_pair_3={'text_x': 0.58, 'text_y': 1.27, 'text': 'AVG', 'value': f"{daily_cdr_avg:,.2f}"},
                      des_pair_4={'text_x': 0.79, 'text_y': 1.27, 'text': 'Total Records', 'value': f"{total_records:,.2f}"})

        q_total_daily_uid = "select to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' )) as date, " \
                            "count(distinct uid) as total_users from {provider_prefix}_consolidate_data_all group by " \
                            "to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' ))" \
                            .format(provider_prefix=self.provider_prefix)

        self.cursor.execute(q_total_daily_uid)

        row_total_daily_uid = self.cursor.fetchall()
        total_daily_uid_x = []
        total_daily_uid_y = []
        for row in row_total_daily_uid:
            total_daily_uid_x.append(row[0])
            total_daily_uid_y.append(row[1])

        q_total_daily_uid_all = "select min(total_users), max(total_users), avg(total_users) from ({}) td".format(
            q_total_daily_uid)
        self.cursor.execute(q_total_daily_uid_all)

        row_total_daily_uid_all = self.cursor.fetchall()
        daily_uid_min, daily_uid_max, daily_uid_avg = row_total_daily_uid_all[0][0], row_total_daily_uid_all[0][1], \
                                                      row_total_daily_uid_all[0][2]
        print(row_total_daily_uid)
        hp.make_graph(total_daily_uid_x, 'Date', total_daily_uid_y, 'Total Users', 'Daily Unique Users', '{}/daily_unique_users'.format(self.graph_location) ,
                      des_pair_1={'text_x': 0.075, 'text_y': 1.27, 'text': 'MIN', 'value': f"{daily_uid_min:,.2f}"},
                      des_pair_2={'text_x': 0.33, 'text_y': 1.27, 'text': 'MAX', 'value': f"{daily_uid_max:,.2f}"},
                      des_pair_3={'text_x': 0.58, 'text_y': 1.27, 'text': 'AVG', 'value': f"{daily_uid_avg:,.2f}"},
                      des_pair_4={'text_x': 0.79, 'text_y': 1.27, 'text': 'Total Unique IDs',
                                  'value': f"{total_uids:,.2f}"})

        q_total_daily_locations = "select to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' )) as date, " \
                                  "count(distinct a2.latitude, a2.longitude) as unique_locations from dtac_consolidate_data_all a1 " \
                            "join dtac_cell_tower_data_preprocess a2 on(a1.cell_id = a2.cell_id) " \
                            "group by to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd'))".format(provider_prefix=self.provider_prefix)

        self.cursor.execute(q_total_daily_locations)

        row_total_daily_locations = self.cursor.fetchall()
        total_daily_location_x = []
        total_daily_location_y = []
        for row in row_total_daily_locations:
            total_daily_location_x.append(row[0])
            total_daily_location_y.append(row[1])

        q_total_daily_location_all = "select min(unique_locations), max(unique_locations), avg(unique_locations) from ({}) td".format(
            q_total_daily_locations)
        self.cursor.execute(q_total_daily_location_all)

        row_total_daily_location_all = self.cursor.fetchall()
        daily_location_min, daily_location_max, daily_location_avg = row_total_daily_location_all[0][0], row_total_daily_location_all[0][1], \
                                                      row_total_daily_location_all[0][2]
        print(row_total_daily_uid)
        hp.make_graph(total_daily_location_x, 'Date', total_daily_location_y, 'Total Locations', 'Daily Unique Locations', '{}/daily_unique_locations'.format(self.graph_location),
                      des_pair_1={'text_x': 0.075, 'text_y': 1.27, 'text': 'MIN', 'value': f"{daily_location_min:,.2f}"},
                      des_pair_2={'text_x': 0.33, 'text_y': 1.27, 'text': 'MAX', 'value': f"{daily_location_max:,.2f}"},
                      des_pair_3={'text_x': 0.58, 'text_y': 1.27, 'text': 'AVG', 'value': f"{daily_location_avg:,.2f}"},
                      des_pair_4={'text_x': 0.79, 'text_y': 1.27, 'text': 'Total Unique Locations',
                                  'value': f"{total_unique_locations:,.2f}"})

        q_total_daily_avg_cdr = "select date, total_records/total_uids as daily_average_cdr from(select to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' )) as date, " \
                                  "count(distinct uid) as total_uids, count(*) as total_records from dtac_consolidate_data_all a1 " \
                                  "group by to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' )))td1".format(
            provider_prefix=self.provider_prefix)

        self.cursor.execute(q_total_daily_avg_cdr)

        row_total_daily_avg_cdr = self.cursor.fetchall()
        total_daily_avg_cdr_x = []
        total_daily_avg_cdr_y = []
        for row in row_total_daily_avg_cdr:
            total_daily_avg_cdr_x.append(row[0])
            total_daily_avg_cdr_y.append(row[1])

        q_total_daily_location_all = "select avg(daily_average_cdr) from ({}) td".format(
            q_total_daily_avg_cdr)
        self.cursor.execute(q_total_daily_location_all)

        row_total_daily_avg_cdr_all = self.cursor.fetchall()
        daily_avg_cdr = row_total_daily_avg_cdr_all[0][0]
        print(row_total_daily_avg_cdr)
        hp.make_graph(total_daily_avg_cdr_x, 'Date', total_daily_avg_cdr_y, 'Total Daily Average CDRs', 'Daily Average CDRs', '{}/daily_avg_cdr'.format(self.graph_location),
                      des_pair_1={'text_x': 0.035, 'text_y': 1.27, 'text': 'Total Daily Avg CDRs',
                                  'value': f"{daily_avg_cdr:,.2f}"})

        q_total_daily_avg_locations = "select date, unique_locations/unique_users as daily_avg_locations, unique_cell_ids/unique_users as daily_avg_cell_ids " \
                                      "from (select to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd' )) as date, " \
                                  "count(distinct a2.latitude, a2.longitude)  as unique_locations , count(distinct a1.uid) as unique_users, " \
                                  "count(distinct a1.cell_id) as unique_cell_ids from dtac_consolidate_data_all a1 " \
                                  "join dtac_cell_tower_data_preprocess a2 on(a1.cell_id = a2.cell_id) " \
                                  "group by to_date(from_unixtime(unix_timestamp(call_time ,'yyyyMMdd hh:mm:ss'), 'yyyy-MM-dd'))) td1".format(
            provider_prefix=self.provider_prefix)

        self.cursor.execute(q_total_daily_avg_locations)

        row_total_daily_avg_locations = self.cursor.fetchall()
        total_daily_avg_location_x = []
        total_daily_avg_location_y = []
        for row in row_total_daily_avg_locations:
            total_daily_avg_location_x.append(row[0])
            total_daily_avg_location_y.append(row[1])

        q_total_daily_avg_location_all = "select avg(td.daily_avg_cell_ids), avg(td.daily_avg_locations) from ({}) td".format(
            q_total_daily_avg_locations)
        self.cursor.execute(q_total_daily_avg_location_all)

        row_total_daily_location_all = self.cursor.fetchall()
        daily_avg_location_cell_ids, daily_avg_location = row_total_daily_location_all[0][0], \
                                                                     row_total_daily_location_all[0][1]
        print(row_total_daily_avg_locations)
        hp.make_graph(total_daily_avg_location_x, 'Date', total_daily_avg_location_y, 'Total Unique Locations', 'Daily Unique Average Locations',
                      '{}/daily_unique_avg_locations'.format(self.graph_location),
                      des_pair_1={'text_x': 0.00, 'text_y': 1.27, 'text': 'Avg Daily Unique Cell IDs ',
                                  'value': f"{daily_avg_location_cell_ids:,.2f}"},
                      des_pair_2={'text_x': 0.28, 'text_y': 1.27, 'text': 'Avg Daily Unique Locations',
                                  'value': f"{daily_avg_location:,.2f}"})


# argument parser
parser = argparse.ArgumentParser(description='Argument indicating the configuration file')

# add configuration argument
parser.add_argument("-c", "--config", help="add a configuration file you would like to process the cdr data"
                                           " \n ex. py py_hive_connect.py -c config.json",
                    action="store")

# parse config to args.config
args = parser.parse_args()
visualizer = CDRVisualizer(args.config)
#
# set hive.exec.dynamic.partition.mode=nonstrict;
#    set hive.support.concurrency=true;
#    set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
#    set hive.compactor.initiator.on=true;
#    set hive.compactor.worker.threads=1;
#    set hive.exec.dynamic.partition=true;
#    set hive.exec.max.dynamic.partitions.pernode = 4000;
#    set hive.exec.max.created.files = 150000;
#    set hive.enforce.bucketing=true;
#    set mapred.map.tasks.speculative.execution=false;
#    set mapred.reduce.tasks.speculative.execution=false;
#    set hive.mapred.reduce.tasks.speculative.execution=false;
#    set hive.map.aggr=false;
#    set mapred.reduce.slowstart.completed.maps=0.98;
#    set mapred.job.reuse.jvm.num.tasks=50;
#    set hive.support.sql11.reserved.keywords=false;
#    use cdrproject;
