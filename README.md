# CDR-analysis-tools-hadoop

Like the standalone version, this repository is a set of tools written in Python for analyzing Call Detail Records (CDRs) data, additionally based on the hadoop platform which supports a large amount of data. The analysis includes Visualization (with reports and processed data compatible with other visualization platforms), Origin-Destination (OD) and Interpolation. 

This repository will be incrementally updated from time to time. Kindly visit the repository and have a look at this file. 


## Getting Started

These instructions will get you a copy of the software package and running on your local machine.
It can be run on both Windows and Linux. The tool dependencies are in the requirements.txt file 
which can be installed in 1 command.

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
After preparing the data, see:

* [Statistics Report](../master/Statistics/)
* [Origin Destination](../master/Origin_Destination/)
* [Interpolation](../master/Interpolation)

## Data preparation
The user needs 2 files for the tool, a cdr file and a location mapping file. Both of them come with different column names and formats. To process CDR data, the data needs to be in the format that is compatible with the tools. The mapping json file maps from your prepared raw csv files to Hive tables ready for the processing and a mapping scheme for each file has to be done by the user. 

### a CSV file for CDR records
To analyse the CDR data, the user needs to provide the tools with a CDR file in the csv format. It needs to contain
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

For 9 and 10 If not existing, then it will be mapped in the cell tower mapping file by cell_id 

#### Mapping
Given a CDR file, the mapping in the key "cdr_data_layer" in the file config.json is shown below

```
"cdr_data_layer":[
		{"input_no":1, "input_name":"SUBID", "data_type":"String",  "output_no":1,   "name":"UID", "custom": ""},
		{"input_no":-1, "input_name":"IMEI", "data_type":"String",  "output_no":2,   "name":"IMEI", "custom": ""},
		{"input_no":-1, "input_name":"IMSI", "data_type":"String",  "output_no":3,   "name":"IMSI", "custom": ""},
		{"input_no":2, "input_name":"CDATE", "data_type":"String",  "output_no":-1,   "name":"CDATE", "custom": ""},
		{"input_no":3, "input_name":"CTIME", "data_type":"String",  "output_no":4,   "name":"CALL_TIME",  "custom": "CONCAT(CDATE,' ',CTIME)"},
		{"input_no":4, "input_name":"DURATION", "data_type":"String",  "output_no":5,   "name":"DURATION", "custom": ""},
		{"input_no":5, "input_name":"CELLID", "data_type":"String",  "output_no":6,   "name":"CELL_ID", "custom": ""},
		{"input_no":6, "input_name":"LATITUDE", "data_type":"String",  "output_no":7,   "name":"LATITUDE", "custom": ""},
		{"input_no":7, "input_name":"LONGITUDE", "data_type":"String",  "output_no":8,   "name":"LONGITUDE", "custom": ""},
		{"input_no":9, "input_name":"NETWORK_TYPE", "data_type":"String",  "output_no":9,   "name":"NETWORK_TYPE", "custom": ""},
		{"input_no":10, "input_name":"CALL_TYPE", "data_type":"String",  "output_no":10,   "name":"CALL_TYPE", "custom": ""}
 ],
 ```
* To map, If the column in the raw file reflects one of the required columns for CDR (ex. UID) then put it in the configuration item
whose "name" field contains "UID" (ex. "SUBID") and both "input_no" and "output_no" will be not "-1"
* Do not remove the configuration item. If there is no raw column corresponding to the required column, set "input_no" -1 
meaning that there is no CALL_TYPE column in the raw file
    * For example, if you don't have call_type column in your raw cdr file, the configuration will be
        * {"input_no":-1, "input_name":"CALL_TYPE", "data_type":"String",  "output_no":-1,   "name":"CALL_TYPE", "custom": ""}
* All the columns in the raw file needs to be indicated even it is not mapped to the required column. Simply adding more configuration items. If it is not mapped to any required column,
  set "output_no" -1 (ex. "CDATE") meaning that the column "CDATE" in the raw file does not reflect any required columns
* Some column may need more function to convert into a desirable format, you can indicate in "custom" field
    * For example, in the fifth item, "custom" contains CONCAT(CDATE, ' ', CTIME). The column CTIME is in the raw so "input_no" is not -1".
    and operands must be in the item in which the "input_no" is not -1 (it exists in the raw file)

### a CSV location mapping file for administration units
The previous csv file will be joined with this cell id file to calculate zone-based statistics. It should supply
1. Cell ID (will be joined with the Cell ID in the CDR record file)
2. At least one Administration Unit (ex. province or district) name
3. Latitude
4. Longitude

```
CELL_ID      : Unique Cell Tower ID (LAC+CellID)
Longitude    : Real Number (decimal degree) in WGS84
Latitude     : Real Number(decimal degree) in WGS84
At least one Administration Unit (ex. province or district) Name
```

#### Mapping
The mapping in this file needs to be done in the same way as previously mentioned in the CDR raw file.

```
	"cdr_cell_tower":[
		{"input_no":1, "input_name":"bs_seq", "data_type":"String",  "output_no":-1,   "name":"BS_SEQ"},
		{"input_no":2, "input_name":"cell_seq", "data_type":"String",  "output_no":1,   "name":"CELL_ID" },
		{"input_no":3, "input_name":"name", "data_type":"String",  "output_no":-1,   "name":"NAME"},
		{"input_no":4, "input_name":"lac", "data_type":"String",  "output_no":-1,   "name":"CELLNAME" },
		{"input_no":5, "input_name":"cell", "data_type":"String",  "output_no":-1,   "name":"CI" },
		{"input_no":6, "input_name":"lon", "data_type":"String",  "output_no":2,   "name":"LATITUDE" },
		{"input_no":7, "input_name":"lat", "data_type":"String",  "output_no":3,   "name":"LONGITUDE" },
		{"input_no":8, "input_name":"ISO2", "data_type":"String",  "output_no":-1,   "name":"ISO2" },
		{"input_no":9, "input_name":"NAME_0_2", "data_type":"String",  "output_no":-1,   "name":"NAME_0_2"},
		{"input_no":10, "input_name":"ID_1_2", "data_type":"String",  "output_no":-1,   "name":"ID_1_2" },
		{"input_no":11, "input_name":"NAME_1_2", "data_type":"String",  "output_no":4,   "name":"ADMIN0", "geojson_filename": "", "geojson_col_name": "" },
		{"input_no":12, "input_name":"ID_2", "data_type":"String",  "output_no":-1,   "name":"ID2" },
		{"input_no":13, "input_name":"NAME_2", "data_type":"String",  "output_no":5,   "name":"ADMIN1",  "geojson_filename": "", "geojson_col_name": ""  },
		{"input_no":14, "input_name":"ENGTYPE_2", "data_type":"String",  "output_no":-1,   "name":"ENGTYPE_2" }
	]
```

One difference is that you need to supply at least one administration unit or your interested location to calculate zone population. For example, in the "input_no" 11, it contains an administration unit data
and is mapped to "ADMIN0" (administration unit 0). It needs to be in the format ADMIN[0-5] to make the tool work (you may have shopping complex names in "input_name" and name it "ADMIN0" for example).
If you want to visualize, put your geojson file location in the "geojson_filename" and the data will be joined with the zone population data and can be visualized in [kepler.gl](https://kepler.gl)

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

"host": "host_name", **hostname of the hadoop server**

"port": 10000, **hive2 server port**

"frequent_location_percentage": 80, **sum of the frequent location of a particular uid**

"csv_location": "csv_reports", **directory of the output csv reports**

"graph_location": "graphical_reports", **directory of the graph reports**

"od_admin_unit": "admin1", **administration unit used for calculating origin-destination**

"od_date": "2016-03-01", **date selected for origin-destination**

"interpolation_poi_file_location": "/path/to/poi", **a poi file for interpolation**
	
"interpolation_osm_file_location": "/path/to/osm", **an osm file for interpolation**
	
"interpolation_voronoi_file_location": "path/to/voronoi", **a voronoi file for interpolation**


"cdr_data_layer": [...], the mapping scheme of cdr raw file to the table used for processing

"cdr_cell_tower": [...], the mapping scheme of cell tower mapping raw file to the table used for processing

## Prerequisites
  * Hadoop server with Hive installed
  * Python 3 or above 
  * Python pip3 (a Python package installer)
  
## Installation
clone the repository and then
install all requirement packages in requirements.txt using command 
  * pip install -r requirements.txt
  
## Preparing tables

Go to [config.json](sample_configs/config.json) or [config_big.json](sample_configs/config_big.json) to see the configuration files to setup the variables and 
start mapping your data.

Then go to [run_prepare_cdr_and_mapping.py](run_prepare_cdr_and_mapping.py) in the user section and run

* python3 run_prepare_cdr_and_mapping.py -c {config_file}

Example

* python3 run_prepare_cdr_and_mapping.py -c sample_configs/config_big.json

You may have some error due to mapping but after fixing it, you can continue from the most current function. If you do not want the tables to be deleted and created again, go to hive_connector.py and then comment the line with function create_tables in the __init__ function
**

There are mainly 3 sections you may want to customize. 

**main() in [run_prepare_cdr_and_mapping.py](run_prepare_cdr_and_mapping.py)**
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
    cdr_data = extract_mapping_data(config)

    # initialize hive and create tables
    table_creator = HiveTableCreator(config, cdr_data)
    table_creator.initialize('hive_init_commands/initial_hive_commands_stats.json')  # init hive
    table_creator.create_tables()

    print('Overall time elapsed: {} seconds'.format(format_two_point_time(start, time.time())))

```
**[hive_create_tables.py](Common/hive_create_tables.py) in \__init\__**
If you don't want tables to be created again (maybe after some errors but tables created), you can comment it in **\__init\__** function

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
## License
Free to use and distribute with acknowledgement.
