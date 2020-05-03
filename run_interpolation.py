from Common.config_object import Config
from Common.cdr_interpolation import Interpolation
from Common.hive_create_tables import HiveTableCreator
from Common.hive_connection import HiveConnection
from Common.helper import format_two_point_time
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
    hc = HiveConnection(host=config.host, port=config.port, user=config.user)

    # initialize hive and create tables
    table_creator = HiveTableCreator(config)
    table_creator.initialize('hive_init_commands/initial_hive_commands_interpolation.json')  # mandatory (init hive)

    # init interpolation generators
    it = Interpolation(config)

    # interpolation
    it.calculate_interpolation()

    print('Overall time elapsed: {} seconds'.format(format_two_point_time(start, time.time())))


if __name__ == '__main__':
    main()
