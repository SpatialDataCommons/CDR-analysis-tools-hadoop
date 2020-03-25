from config_object import Config
from cdr_visualizer import CDRVisualizer
from cdr_data import CDRData
from helper import extract_mapping_data, format_two_point_time
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
    cdr_data = CDRData()
    config = Config(args.config)
    extract_mapping_data(config, cdr_data)
    vs = CDRVisualizer(config, cdr_data)

    # user section here
    # vs.calculate_data_statistics()
    # vs.calculate_daily_statistics()
    # vs.calculate_monthly_statistics()
    #
    # vs.calculate_zone_population()
    # vs.calculate_user_date_histogram()
    vs.calculate_summary()
    # vs.daily_cdrs()
    # vs.daily_unique_users()
    # vs.daily_unique_locations()
    # vs.daily_average_cdrs()
    # vs.daily_unique_average_locations()
    # vs.calculate_od()
    print('Overall time elapsed: {} seconds'.format(format_two_point_time(start, time.time())))

if __name__ == '__main__':
    main()