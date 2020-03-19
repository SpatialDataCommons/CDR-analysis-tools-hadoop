from config_object import Config
from cdr_visualizer import CDRVisualizer
from cdr_data import CDRData
from helper import extract_mapping_data
import argparse


def main():
    # argument parser
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
<<<<<<< HEAD
    # vs.calculate_data_statistics()
    # vs.calculate_daily_statistic()
    # vs.calculate_monthly_statistic()
    # vs.calculate_frequent_locations()
    # vs.calculate_zone_population()
    # vs.calculate_user_date_histogram()
    # vs.calculate_summary()
    vs.calculate_od()
=======
    vs.calculate_data_statistics()
    vs.calculate_daily_statistic()
    vs.calculate_monthly_statistic()
    vs.calculate_frequent_locations()
    vs.calculate_zone_population()
    vs.calculate_user_date_histogram()
    vs.calculate_summary()
    # vs.calculate_od()
>>>>>>> parent of 77b2c27... added big_csv



if __name__ == '__main__':
    main()