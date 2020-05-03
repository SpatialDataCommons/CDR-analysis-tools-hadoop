# Statistics
In this sections, it concerns generating reports, graphs for statistics or CDR data. Output will be in the 
format of CSV file and a graph (png) files
To illustrate, a simple csv file for cdr and mapping will be used and they are located in.
## Prerequisites
Tables obtained from the script [run_prepare_cdr_and_mapping.py](../run_prepare_cdr_and_mapping.py).
See the [first page](../README.md) for how to prepare a CDR file and a cell tower mapping file. The following are the columns 
of the CDR consolidate data table.
* CDR Consolidate Data Table 
```
UID          : Unique Identifier of each user
IMEI          : International Mobile Equipment Identity (IMEI) of Caller
IMSI          : International Mobile Subscriber Identity (IMSI) of Caller
CALL_TIME    : Activity Time (Start Time) in “YYYY-MM-DD HH:mm:ss” format 
DURATION     : Call Duration in seconds
CELL_ID      : Unique Cell Tower ID (LAC+CellID)
CALL_TYPE     : Type of the call (Data, Voice or SMS)
NETWORK_TYPE  : Type of the network (2G, 3G, 4G, 5G)
Longitude    : Real Number (decimal degree) in WGS84
Latitude     : Real Number(decimal degree) in WGS84
```
* Cell Tower Mapping Preprocess Table
```
CELL_ID      : Unique Cell Tower ID (LAC+CellID)
Longitude    : Real Number (decimal degree) in WGS84
Latitude     : Real Number(decimal degree) in WGS84
Admin1       : Administration Unit 1 name (if any)
Admin2       : Administration Unit 2 name (if any)
.
.
.
AdminN       : Administration Unit N name (if any)
```
* Cell Tower Data Admin X Table (For generating sequence numbers of an administration unit in case of duplication)
```
AdminX_ID       : Administration Unit X name
AdminX_Name     : Name of the Administration Unit X
CELL_ID      : Unique Cell Tower ID
Longitude    : Real Number (decimal degree) in WGS84
Latitude     : Real Number (decimal degree) in WGS84
```
## Usage
run command
* python3 [run_statistics.py](../run_statistics.py) -c {config_file}

Example
* python3 run_statistics.py -c [config_big.json](../sample_configs/config_big.json)

If you wish to execute some of the features, you can comment some lines in the file in main() of [run_statistics.py](../run_statistics.py) 
in the user section 

```
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
```    

## Output Reports
The implementation is in [cdr_statistics.py](../Common/cdr_statistics.py)
### Data statistics
Located in calculate_data_statistics(). In the data statistics, the result output (see the [Data statistics](../Statistics/output_reports/css_file_data_stat.csv)) provided will be:

```
Total Records   : the total cdr usage data 
Total Days      : the total days that have usage
Unique id       : the total unique ids of the data (it could be imei, imsi or another identifier given)
Unique imei     : the total unique imeis (will be omitted if it is the unique id already)
Unique imsi     : the total unique imsis (will be omitted if it is the unique id already)
Unique loc name : the total unique latitude and longitude of the cdr data
Start date      : the starting date of the data
End date        : the end date of the data
```
### Daily and Monthly Statistics
Located in calculate_daily_statistics() and calculate_monthly_statistics (see the [daily](../Statistics/output_reports/css_provider_data_stat_daily.csv) and [monthly](../Statistics/output_reports/css_provider_data_stat_monthly.csv) output). Calculating some properties order by date first and then the type of call type and network type.

Each field in the daily statistics is by a particular date (or year and month for monthly statistics), call type and network type including:
```
Date            : the dates that have cdr records
Call Type       : the call type of the cdr data (VOICE, DATA, SMS)
Network Type    : the network type of the cdr data (2G, 3G, 4G)
Total Records   : the total records
Total Days      : the total days
Unique id       : the total unique ids of the data (it could be imei, imsi or another identifier given)
Unique imei     : the total unique imeis 
Unique imsi     : the total unique imsis
Unique loc name : the total unique latitude and longitude of the cdr data
```

### Zone population
at least one administration level is needed to calculate zone population.
By indicating in the mapping in config.json
```
"cdr_cell_tower":[
		{"input_no":1, "input_name":"BTSID", "data_type":"String",  "output_no":1,   "name":"UID"},
		{"input_no":2, "input_name":"SITE_NAME", "data_type":"String",  "output_no":2,   "name":"SITE_NAME"},
		{"input_no":3, "input_name":"LONGITUDE", "data_type":"String",  "output_no":3,   "name":"LONGITUDE"},
		{"input_no":4, "input_name":"LATITUDE", "data_type":"String",  "output_no":4,   "name":"LATITUDE"},
		{"input_no":5, "input_name":"CELLID", "data_type":"String",  "output_no":5,   "name":"CELL_ID" },
		{"input_no":6, "input_name":"CELLNAME", "data_type":"String",  "output_no":-1,   "name":"CELLNAME" },
		{"input_no":7, "input_name":"CI", "data_type":"String",  "output_no":-1,   "name":"CI" },
		{"input_no":8, "input_name":"AZIMUTH", "data_type":"String",  "output_no":-1,   "name":"AZIMUTH" },
USED	{"input_no":9, "input_name":"DISTRICT", "data_type":"String",  "output_no":6,   "name":"ADMIN1", "geojson_filename": "japan.json", "geojson_col_name": "nam"},
USED	{{"input_no":10, "input_name":"PROVINCE", "data_type":"String",  "output_no":7,   "name":"ADMIN2", "geojson_filename": "", "geojson_col_name": ""}
	]
```
You need to indicate that the column name indicating a place means what level of administration. For example,
DISTRICT column is ADMIN1.

If you are able to provide a geojson file for this administration level then put it in the key "geojson_filename" with the name of the field in geojson in "geojson_col_name"
 in order for the tool to join the attribute correctly.
 
The result you get is the zone_base_aggregations_level_ADMIN{X}.csv and you can see the result output [zone_based_aggregations_level_ADMIN1](../Statistics/output_reports/zone_based_aggregations_level_ADMIN1.csv)

which includes:
```
Administration Level x : Your input administration level x
Count Activities       : Total CDR Records that are in the administration level
Count Unique IDs       : Total Unique Ids
```
and {geojson_file_name}_joined_ADMIN{X}.json (you can put this in visualization service such as [kepler.gl](https://kepler.gl))

### Summary Data
For the output summary date [(summary)](../Statistics/output_reports/summary_stats.csv). It contains overall statistics of the cdr data including:
```
Total records
Total unique IDs
Total days          
Average daily usage 
Average daily voice 
Average daily sms   
Average daily unique cell id
Average Daily Admin Level 1
```
### Frequent Locations
Frequent locations (All-day and night) output is the most popular cell_id in which user make a call data in a day or a night.
The output here will be a table named {your_prefix}_frequent_location_thresholded and {your_prefix}_frequent_location_thresholded_night.
Example output

![alt text](https://github.com/shibalab/CDR-analysis-tools-hadoop/blob/master/Statistics/output_reports/frequent_location_output_sample.png "Frequent Locations")


## Output Graphs
The implementation is in [cdr_statistics.py](../Common/cdr_statistics.py)
### User Date Histogram
The output histogram is the histogram of each number of days (x) with the corresponding number of unique ids of users who make a call for the particular number of days (y).
![alt text](https://github.com/shibalab/CDR-analysis-tools-hadoop/blob/master/Statistics/output_graphs/user_data_histogram.png "User Date Histogram")
### Daily CDRs
The graph reports the daily usage of a user each day with minimum, maximum, average and total cdr
![alt text](https://github.com/shibalab/CDR-analysis-tools-hadoop/blob/master/Statistics/output_graphs/daily_cdrs.png "Daily CDRs")
### Daily CDRs by call type
This graph reports the daily usage of a user each day by call type (multiple lines)
![alt text](https://github.com/shibalab/CDR-analysis-tools-hadoop/blob/master/Statistics/output_graphs/daily_cdr_by_call_type.png "Daily CDRs by call type")
### Daily Unique Users
Daily unique uids are reported daily in the graph with statistics information (minimum, maximum, average and total)
![alt text](https://github.com/shibalab/CDR-analysis-tools-hadoop/blob/master/Statistics/output_graphs/daily_unique_users.png "Daily Unique Users")
### Daily Unique Locations
Daily unique locations (unique latitude and longitude) are reported daily in the graph with statistics information (minimum, maximum, average and total)
![alt text](https://github.com/shibalab/CDR-analysis-tools-hadoop/blob/master/Statistics/output_graphs/daily_unique_locations.png "Daily Unique Locations")
### Daily average CDRs
Daily average CDRs are the daily CDR per user in average with the average of them by days displayed on top
![alt text](https://github.com/shibalab/CDR-analysis-tools-hadoop/blob/master/Statistics/output_graphs/daily_avg_cdr.png "Daily Average CDRs")

### Daily Unique Average Locations
This graphs represent daily average locations which are the daily CDR per user in average with the average of them by days being shown on the top of the graph
![alt text](https://github.com/shibalab/CDR-analysis-tools-hadoop/blob/master/Statistics/output_graphs/daily_unique_avg_locations.png "Daily Average CDRs")

