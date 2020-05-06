from Common.config_object import Config
from Common.hive_create_tables import HiveTableCreator
from Common.hive_connection import HiveConnection
from Common.helper import extract_mapping_data, format_two_point_time
from Common.cdr_data import CDRData
import argparse
import time


def main():
    # argument parser
    start = time.time()
    parser = argparse.ArgumentParser(description='Argument indicating the configuration file')

    # add configuration argument
    parser.add_argument("-c", "--config", help="add a configuration file you would like to process the cdr data"
                                               " \n ex. py py_hive_connect.py -c config.json",
                        action="store")

    # parse config to args.config
    args = parser.parse_args()

    config = Config(args.config)
    HiveConnection(host=config.host, port=config.port, user=config.user)
    cdr_data = CDRData()
    extract_mapping_data(config, cdr_data)

    # initialize hive and create tables

    table_creator = HiveTableCreator(config, cdr_data)
    table_creator.initialize('hive_init_commands/initial_hive_commands_stats.json')  # init hive
    table_creator.create_tables()

    print('Overall time elapsed: {} seconds'.format(format_two_point_time(start, time.time())))


if __name__ == '__main__':
    main()
