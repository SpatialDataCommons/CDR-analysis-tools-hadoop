# CDR-analysis-tools-hadoop

Like the standalone version, this repository is a set of tools written in Python for analyzing Call Detail Records (CDRs) data, additionally based on the hadoop platform which supports a large amount of data. The analysis includes Visualization (with reports and processed data compatible with other visualization platforms), Origin-Destination (OD) and Interpolation. 

This repository will be incrementally updated from time to time. Kindly visit the repository and have a look at this file. 

## Getting Started

These instructions will get you a copy of the software package and running on your local machine. It can be run on both Windows and Linux. The tool dependencies are in the requirements.txt file so that the user can install all of them in just 1 command.

Structure this package

```
├─Statistics Report:
│       Generating csv reports and graph reports including
|	  - summary statistics (average usage, voice call and etc.)
│	  - whole data statistics (ex. total cdrs, total days and locations)
|	  - daily and monthly statistics of users
|	  - frequent locations
|	  - zone based aggregation
|	  - graphical daily data
|	  - usage histogram
|
├─Origin-Destination (OD):
│       Generating Origin-Destination file indicating the movement of humans
│
├─Interpolation:
|       A set of software for route interpolation including 
|         - Extracting stay points
|         - Extract tripsegment
|         - Relocation PoI
|         - Route Interpolation with transpotation network
```



## Data preparation
The user needs 2 files for the tool, a cdr file and a location mapping file. Both of them come with different column names and formats. To process CDR data, the data needs to be in the format that is compatible with the tools. The mapping json file maps from your prepared raw csv files to Hive tables ready for the processing and a mapping scheme for each file has to be done by the user. 

### a CSV file for CDR records
To analyse the CDR data, the user needs to provide the tools with a CDR file in the csv format. It needs to contain
1. IMEI or IMSEI 
2. Call start time
3. Cell ID
4. Call Type (2G, 3G, 4G)
5. Network Type (VOICE, DATA, SMS)

#### Mapping
Given a CDR file, the mapping in the key "cdr_data_layer" in the file config.json is shown below

```
"cdr_data_layer":[{"input_no":1, "input_name":"IMEI", "data_type":"String",  "output_no":1,   "name":"UID"},
		{"input_no":2, "input_name":"CDATE", "data_type":"String",  "output_no":-1,   "name":"CALL_DATE"},
		{"input_no":3, "input_name":"CTIME", "data_type":"String",  "output_no":4,   "name":"CALL_TIME",  "custom": "CONCAT(CDATE,' ',CTIME)"},
		{"input_no":4, "input_name":"DURATION", "data_type":"String",  "output_no":5,   "name":"CALL_DURATION"},
		{"input_no":5, "input_name":"CELLID", "data_type":"String",  "output_no":6,   "name":"CELL_ID"},
		{"input_no":6, "input_name":"LATITUDE", "data_type":"String",  "output_no":7,   "name":"LATITUDE"},
		{"input_no":7, "input_name":"LONGITUDE", "data_type":"String",  "output_no":8,   "name":"LONGITUDE"},
		{"input_no":9, "input_name":"NETWORK_TYPE", "data_type":"String",  "output_no":10,   "name":"NETWORK_TYPE"},
		{"input_no":8, "input_name":"CALL_TYPE", "data_type":"String",  "output_no":9,   "name":"CALL_TYPE" }
 ],
 ```
In the mapping data, non-negative "input_no" and "output_no" together mean direct mapping. For example, "DURATION" will be mapped to "CALL_DURATION" in "input_no" 4. The input zone ("input_no", "input_name") is from your raw table and the output zone ("output_no", "name") is the target column name in the preprocessed tables. Value -1 in the input_no or output_no means there is no column in the particular raw table or preprocessed table. 
Value -1 in the "output_no" means it is not going to be put in the preprocessed table as a column and name will be ignored but the "input_no" in the same object can be non-negative to indicate that the "input_name" field will be a column in the raw table. The user needs to supply all the raw input columns to the mapping file. the custom field means there can be a special function applied to one or more than one fields. the "CALL_TIME" column in the preprocessed table will be the concatenation of the CDATE and CTIME columns of the raw table.     

### a CSV location mapping file for administration units
The previous csv file will be joined with this cell id file to calculate zone-based statistics. It should supply
1. Cell ID (will be joined with the Cell ID in the CDR record file)
2. At least one Administration Unit (ex. province or district) name
3. Latitude
4. Longitude

#### Mapping
The mapping in this file needs to be done in the same way as previously mentioned in the CDR raw file.

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
		{"input_no":9, "input_name":"DISTRICT", "data_type":"String",  "output_no":6,   "name":"ADMIN1", "geojson_filename": "japan.json", "geojson_col_name": "nam"},
		{"input_no":9, "input_name":"DISTRICT_ID", "data_type":"String",  "output_no":7,   "name":"DISTRICT_ID" }
	]
```

One difference is that you need to supply at least one administration unit or your interested location to calculate zone population. For examplle, in the "input_no" 9, it contains district and is mapped to "ADMIN1" (administration unit 1). It needs to be in the format ADMIN[0-5] to make the tool work (you may need to have shoppnig complex names in "input_name" and name it "ADMIN0" for example).
If you want to visualize, put your geojson file location in the "geojson_filename" and the data will be joined with the zone population data and can be visualized in https://kepler.gl

## Configuration
In config.json file, you need to assign the right path, prefix, location and so on. Here is an example of a config.json file with an explanation 


"hadoop_data_path":"/path/to/cdr/and/celltower/file",

"provider_prefix":"pref1" **any prefix you'd like to name (you may need in case that you want to use this tool to different data** 

"db_name" : "cdrproject", 

"input_delimiter":",", **raw file delimiter (ex. comma "," or tab "\t")**

"input_files" :["cdr.csv"], ** raw cdr file(s)**

"input_file_time_format": "yyyyMMdd hh:mm:ss", **time format in your data (if it is ambiguous ex. no separator between month and year, you need to put the format here or left it blank if it is dash, slash-separated date and colon-separated time**

"input_file_have_header": 1, **if having table description (column names) put 1, otherwise 0**

"input_cell_tower_files" : ["cdr_cell_tower.csv"], **cell tower mapping data**

"input_cell_tower_delimiter":",", 

"input_cell_tower_have_header": 1,

"check_duplicate": true, **filter duplicate rows or not**

"check_invalid_lat_lng": true, **filter invalid lat and lng**

"host": "hadoopmaster.apichon.com", **hostname of the hadoop server**

"port": 10000, **hive2 server port**

"frequent_location_percentage": 80, **sum of the frequent location of a particular uid**

"csv_location": "csv_reports", **directory of the output csv reports**

"graph_location": "graphical_reports", **directory of the graph reports**

## Prerequisites
  * Hadoop server with Hive installed
  * Python 3 or above 
  * Python pip3 (a Python package installer)
  
## Installation
clone the repository and then
install all requirement packages in requirements.txt using command 
  * pip install -r requirements.txt
  
## Usage

Go to config.json or config_big.json to see the configuration files to setup the variables and 
start mapping your data.

Then go to main.py in the #user section and run

$python3 main.py -c {config_file}

Each command is self-explanatory. You may have some error due to mapping but after fixing it, you can continue from the most current function. If you do not want the tables to be deleted and created again, go to hive_connector.py and then comment the line with function create_tables in the __init__ function
**

There are mainly 3 sections you may want to customize. 

**main.py in main()**
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
    cdr_data = CDRData()
    config = Config(args.config)
    extract_mapping_data(config, cdr_data)
    vs = CDRVisualizer(config, cdr_data)

    ### if you don't want any of these to be executed, you can comment it
    # user section here, 
    #ex.
    # vs.calculate_data_statistics() << COMMENT HERE  
    vs.calculate_daily_statistic()
    vs.calculate_monthly_statistic()
    #
    vs.calculate_zone_population()
    vs.calculate_user_date_histogram()
    vs.calculate_summary()
    # vs.calculate_od()
    print('Overall time elapsed: {} seconds'.format(format_two_point_time(start, time.time())))
```
**cdr_visualizer.py in \__init\__**
If you dont want tables to be created again (maybe after some errors but tables created), you can comment it in **\__init\__** function

```
    def __init__(self, config, data):
        self.__dict__ = config.__dict__
        self.hive = HiveConnector(config)
        timer = time.time()
        print('########## Initilizing Hive ##########')
        self.hive.initialize(config)
        print('########## Done. Time elapsed: {} seconds ##########'.format(hp.format_two_point_time(timer, time.time())))
        timer = time.time()
        print('########## Creating Tables ##########')
        #self.hive.create_tables(config, data) <<<< COMMENT HERE
        print('########## Done create all tables. Time elapsed: {} seconds ##########'.format(hp.format_two_point_time(timer, time.time())))
```
**cdr_visualizer.py in calculate_summary()**
You may want to comment some function after calculating the summary.
if you don't want some graphs to be outputted.
At the bottom of the function
```
timer = time.time()

        # self.daily_cdrs(total_records) <<<< COMMENT HERE

        self.daily_unique_users(total_uids)

        self.daily_unique_locations(total_unique_locations)

        self.daily_average_cdrs()

        self.daily_unique_average_locations()
        print('########## FINISHED CALCULATING SUMMARY ##########')
```

## License
Free to use and distribute with acknowledgement.
