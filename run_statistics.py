from Common.config_object import Config
from Common.cdr_statistics import Statistics
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
    HiveConnection(host=config.host, port=config.port, user=config.user)

    table_creator = HiveTableCreator(config)
    table_creator.initialize('hive_init_commands/initial_hive_commands_stats.json')  # mandatory (init hive)

    # init stats generators
    st = Statistics(config)

    # user section here
    # reports
    st.calculate_data_statistics()
    st.calculate_daily_statistics()
    st.calculate_monthly_statistics()
    st.calculate_zone_population()
    st.calculate_summary()
    st.calculate_user_date_histogram()
    # graphs
    st.daily_cdrs()
    st.daily_unique_users()
    st.daily_unique_locations()
    st.daily_average_cdrs()
    st.daily_unique_average_locations()

    # frequent locations (Report)
    st.frequent_locations()
    st.frequent_locations_night()

    # Prerequisite for Origin-Destination, if not wishing to calculate OD, kindly comment the code
    st.rank1_frequent_locations()  # Require frequent_locations() in run_statistics.py

    print('Overall time elapsed: {} seconds'.format(format_two_point_time(start, time.time())))
if __name__ == '__main__':
    main()