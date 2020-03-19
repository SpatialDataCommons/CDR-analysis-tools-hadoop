from impala.dbapi import connect
from helper import json_file_to_object
import os


class HiveConnector:
    def __init__(self, config):
        # username is mandatory
        self.conn = connect(config.host, config.port, user='rsstudent', auth_mechanism='PLAIN')
        self.cursor = self.conn.cursor()

    def initialize(self, config, data):
        for command in json_file_to_object('initial_hive_commands.json')['hive_commands']:
            self.cursor.execute(command)
        if not os.path.exists(config.csv_location):
            os.makedirs(config.csv_location)
        if not os.path.exists(config.graph_location):
            os.makedirs(config.graph_location)

    def create_tables(self, config, data):
        self.import_cell_tower_data_raw(config, data)
        self.preprocess_cell_tower_data(config, data)
        self.import_raw(config, data)
        self.preprocess_data(config, data)
        self.consolidate_table(config, data)

    def import_cell_tower_data_raw(self, config, data):
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_raw'
                            .format(provider_prefix=config.provider_prefix))

        self.cursor.execute('CREATE TABLE {provider_prefix}_cell_tower_data_raw'
                            .format(provider_prefix=config.provider_prefix) +
                            "({})".format(', '.join(data.arg_cell_raw)) +
                            'ROW FORMAT DELIMITED ' +
                            "FIELDS TERMINATED BY '{field_delimiter}' ".format(field_delimiter=config.input_cell_tower_delimiter) +
                            "LINES TERMINATED BY '\n' " +
                            "STORED AS TEXTFILE " +
                            'tblproperties ("skip.header.line.count"="1")')

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

    def import_raw(self, config, data):
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_raw'
                            .format(provider_prefix=config.provider_prefix))
        self.cursor.execute('CREATE TABLE {provider_prefix}_raw'
                            .format(provider_prefix=config.provider_prefix) +
                            "({})".format(', '.join(data.arg_cdr_raw)) +
                            'ROW FORMAT DELIMITED ' +
                            "FIELDS TERMINATED BY '{field_delimiter}' ".format(field_delimiter=config.input_delimiter) +
                            "LINES TERMINATED BY '\n' " +
                            "STORED AS TEXTFILE " +
                            'tblproperties ("skip.header.line.count"="1")')

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

    def preprocess_cell_tower_data(self, config, data):
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_cell_tower_data_preprocess'.format(
            provider_prefix=config.provider_prefix))
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {provider_prefix}_cell_tower_data_preprocess'.format(
            provider_prefix=config.provider_prefix) +
                            "({})".format(', '.join(data.arg_cell_create)) +
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
                            .format(provider_prefix=config.provider_prefix) +
                            "select {} ".format(', '.join(data.arg_cell_map)) +
                            "from {provider_prefix}_cell_tower_data_raw"
                            .format(provider_prefix=config.provider_prefix))

    def preprocess_data(self, config, data):
        print('### Creating preprocessing table if not existing ###')
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_preprocess'.format(provider_prefix=config.provider_prefix))
        self.cursor.execute(
            'CREATE TABLE {provider_prefix}_preprocess'.format(provider_prefix=config.provider_prefix) +
            "({})".format(', '.join(data.arg_cdr_prep)) +
            'ROW FORMAT DELIMITED ' +
            "FIELDS TERMINATED BY ',' " +
            "LINES TERMINATED BY '\n' " +
            "STORED AS SEQUENCEFILE")

        # need username to get privilege
        print('### Inserting into preprocessing  table ###')
        self.cursor.execute("INSERT OVERWRITE TABLE  {provider_prefix}_preprocess "
                            .format(provider_prefix=config.provider_prefix) +
                            "select {} ".format(', '.join(data.arg_cdr_map)) +
                            "from {provider_prefix}_raw"
                            .format(provider_prefix=config.provider_prefix))

    def consolidate_table(self, config, data):
        self.cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_consolidate_data_all'.format(
            provider_prefix=config.provider_prefix))

        create_query = "CREATE TABLE {provider_prefix}_consolidate_data_all".format(
            provider_prefix=config.provider_prefix) + \
                       "({})".format(' ,'.join(data.arg_cdr_prep)) + \
                       "PARTITIONED BY (pdt string) " + \
                       "ROW FORMAT DELIMITED " + \
                       "FIELDS TERMINATED BY ','" + \
                       "LINES TERMINATED BY '\n'" + \
                       'STORED AS SEQUENCEFILE'
        self.cursor.execute(create_query)
        print(data.arg_cdr_con)
        print('### Inserting into the consolidate table ###')
        insert_query = "INSERT INTO TABLE  {provider_prefix}_consolidate_data_all ".format(
            provider_prefix=config.provider_prefix) + \
                       "PARTITION (pdt) select {}, ".format(', '.join(data.arg_cdr_con)) + \
                       "(call_time) as pdt " + \
                       "from {provider_prefix}_preprocess".format(provider_prefix=config.provider_prefix)
        print(insert_query)
        self.cursor.execute(insert_query)
