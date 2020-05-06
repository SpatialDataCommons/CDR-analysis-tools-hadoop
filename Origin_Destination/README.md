# Origin Destination
The tool performs calculation of origin destination based on the data provided in the form of
the consolidate data table. 
Results are the origin destination of a selected date stored in a tsv file.

## Prerequisites
Tables obtained from the script [run_prepare_cdr_and_mapping.py](../run_prepare_cdr_and_mapping.py).
See the [first page](../README.md) for how to prepare a CDR file and a cell tower mapping file. The following are the columns 
of the CDR consolidate data table.
* CDR Consolidate Data Table ({provider_prefix}_consolidate_data_all)
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
* Cell Tower Mapping Preprocess Table ({provider_prefix}_cell_tower_data_preprocess)
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
* Cell Tower Data Admin X Table ({provider_prefix}_cell_tower_data_adminX)
    * for generating sequence numbers of an administration unit in case of duplication
```
AdminX_ID       : Administration Unit X name
AdminX_Name     : Name of the Administration Unit X
CELL_ID      : Unique Cell Tower ID
Longitude    : Real Number (decimal degree) in WGS84
Latitude     : Real Number (decimal degree) in WGS84
```

* CDR Home Location Table ({provider_prefix}_la_cdr_uid_home) 
    * Obtained from finding rank1_frequent_location in calculate data statistics [run_statistics.py](../run_statistics.py))

```
UID 	        : Unique Identifier of each user
SITE_ID 	    : Unique Concatenation of Latitude and Longitude
TCOUNT 	        : Number of records found in a particular SITE_ID
TRANK 	        : The rank of the SITE_ID in how frequent each user is in the SITE_ID area 
PPERCENT        : The ratio of how many times each user have a cdr data among all places at which the user use a mobile device in percentage  	
LONGITUDE 	    : Real Number (decimal degree) in WGS84 
LADITUDE 	    : Real Number (decimal degree) in WGS84
ADMINX_ID       : Administration Unit X name
```

# Configuration
The configuration of the connection of hadoop server and selection needs to be set prior to this.
See the [first page](../README.md) in the configuration section.
Then, in the config file, set two following fields:
* od_admin_unit: set to be a value in ("admin0", "admin1", "admin2", "admin3", "admin4", "admin5") 
according to what you have in the cell tower mapping raw data
* od_date: set to the date you want to perform origin destination to (format "yyyy-mm-dd")

For example, see [config_big.json](../sample_configs/config_big.json) in line 31 and 32
 
# Origin Destination 
run the following command
* python3 [run_origin_destination.py](../run_origin_destination.py) -c {config_file}

Example

* run python3 run_origin_destination.py -c [sample_configs/config_big.json](../sample_configs/config_big.json)

To edit further, the user can go to [cdr_origin_destination.py](../Common/cdr_origin_destination.py) in  
calculate_od()

If only some of the operations are needed, you can comment them here (ex. already finished some steps)

```
        self.cdr_by_uid()
        self.create_od()
        self.create_od_detail()
        self.create_od_sum()
```


# Origin Destination Output
The output will be generated inside the hadoop server in /tmp/hive/od_result. 
The file will have no extension but it can be renamed to have .tsv extension and run without any problem.
The following file is the output sample for the [origin-destination](output_sample/origin_destination.tsv) 
In the file, each field is separated by comma and having the following column name

pdt,origin ,
destination,cast(tcount as string),cast(tusercount as string))

```
1. Date
    •   The date of the origin destination data (as indicated in the config file in the field "od_date")
2. Origin Admin X ID
    •	The origin place in the form of Admin X ID
3. Destination Admin X ID
    •	The destination place in the form of Admin X ID 
4. Count
    •	Number of records of the movement from the origin to destination
5. User Count
    •	Total Users in a particular movement
```
Example

```
2016-03-01	0	805	1.0	1.0
2016-03-01	0	937	4.0	4.0
2016-03-01	0	938	1.0	1.0
2016-03-01	0	940	4.0	4.0
2016-03-01	1001	1062	6.0	6.0
2016-03-01	1001	1064	7.0	7.0
2016-03-01	1001	1065	4.0	4.0
2016-03-01	1001	1082	1.0	1.0
```