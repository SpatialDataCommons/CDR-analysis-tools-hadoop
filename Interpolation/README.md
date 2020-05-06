# Interpolation
The tool will perform the following operations: 
* Trip segmentation
* Stay Point Reallocation
* Route Interpolation

Results are the route interpolation of a CDR data.
## Prerequisites
* CDR Consolidate Data Table ({provider_prefix}_consolidate_data_all)
    * obtained from the script [run_prepare_cdr_and_mapping.py](../run_prepare_cdr_and_mapping.py).
See the [first page](../README.md) for how to prepare a CDR file and a cell tower mapping file. The following are the columns 
of the CDR consolidate data table. * means the column is required to have some valid values.
```
*UID          : Unique Identifier of each user
IMEI          : International Mobile Equipment Identity (IMEI) of Caller
IMSI          : International Mobile Subscriber Identity (IMSI) of Caller
*CALL_TIME    : Activity Time (Start Time) in “YYYY-MM-DD HH:mm:ss” format 
*DURATION     : Call Duration in seconds
*CELL_ID      : Unique Cell Tower ID (LAC+CellID)
CALL_TYPE     : Type of the call (Data, Voice or SMS)
NETWORK_TYPE  : Type of the network (2G, 3G, 4G, 5G)
*Longitude    : Real Number (decimal degree) in WGS84
*Latitude     : Real Number(decimal degree) in WGS84
```
* Building/POIs for Reallocation
* OSM road network data
* Voronoi data of Cell tower/Base Station location

# Configuration
The configuration of the connection of hadoop server and selection needs to be set prior to this.
See the [first page](../README.md) in the configuration section.
Then, in the config file, set five following fields:
* interpolation_poi_file_location: set to be the local path of the poi file
* interpolation_osm_file_location: set to be the local path of the osm file
* interpolation_voronoi_file_location: set to be the local path of the voronoi file
* max_size_cdr_by_uid: set to be the maximum array size of cdr of each particular user
* max_size_interpolation: set to be the max size of interpolation
according to what you have in the cell tower mapping raw data

For example, see [config_big.json](../sample_configs/config_big.json) in line from 34 to 38

# Route Interpolation
run the following command
* python3 [run_interpolation.py](../run_interpolation.py) -c {config_file}

Example

* run python3 run_interpolation.py -c sample_configs/config_big.json

To edit further, the user can go to [cdr_interpolation.py](../Common/cdr_interpolation.py) in  
calculate_interpolation()

If only some of the operations are needed, you can comment them here (ex. already finished some steps)

```
        self.convert_cdr_to_array_format()
        self.create_trip_format()
        self.create_trip_24hr_padding()
        self.create_poi_relocation()
        self.create_route_interpolation()
        self.export_to_csv()
```


# Route Interpolation Output
The output will be generated inside the hadoop server in /tmp/hive/csv_interpolation. 
The file will have no extension but it can be renamed to have .csv extension and run without any problem.
The following file is the output sample for the [interpolation](output_sample/interpolation.csv) 
In the file, each field is separated by comma and having the following column name
```
1. User Id
    •   Unique for each device
2. Trip Sequence
    •	Order of sub trip in a day, start from 1
3. Mobility Type
    •	Value: STAY or MOVE
4. Transportation Mode
    •	Indicate mode of transportation of corresponding sub trip
    •	Value: STAY, WALK, VEHICLE
5. Total Distance 
    •	Total travel distance of sub trip in meters
6. Total Time
    •	Total travel time of sub trip in seconds
7. Start Time 
    •	Indicate start time of sub trip
    •	Format: hh:MM:ss
    •	Example: 8:38:08
8. End Time
    •	Indicate end time of sub trip
    •	Format: hh:MM:ss
    •	Example: 9:30:20 
9. Total Points 
    •	Indicate total number of point data in sub trip
10. Subtrip Sequence
    •	The point number of each sub trip in a day, start from 1
11. Subtrip Point Start Time 
    •	Indicate start time of sub trip
    •	Format: MM/DD/YYYY hh:MM
    •	Example: 1/2/2019 8:38 
12. Subtrip Point Latitude
    •	Indicate the latitude of a particular point in a subtrip
    •	Format: MM/DD/YYYY hh:MM
    •	Example: 1/2/2019 8:38 
13. Subtrip Point Longitude
    •	Indicate the longtitude of a particular point in a subtrip
    •	Format: MM/DD/YYYY hh:MM
    •	Example: 1/2/2019 8:38 
```
Example (replace comma by tab for better illustration)

```
1031073514	1	STAY	STAY	31088	0	0:00:00	8:38:08	1	1	1/2/2019 0:00	23.614079	89.361402
1031073514	2	MOVE	WALK	3132	4330.51	8:38:08	9:30:20	53	1	1/2/2019 8:38	23.619423	89.367677
1031073514	2	MOVE	WALK	3132	4330.51	8:38:08	9:30:20	53	2	1/2/2019 8:39	23.618943	89.368309
1031073514	2	MOVE	WALK	3132	4330.51	8:38:08	9:30:20	53	3	1/2/2019 8:40	23.618462	89.368942
1031073514	2	MOVE	WALK	3132	4330.51	8:38:08	9:30:20	53	4	1/2/2019 8:41	23.617982	89.369574

```