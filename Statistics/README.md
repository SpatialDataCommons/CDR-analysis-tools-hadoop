# Statistics
In this sections, it concerns generating reports, graphs for statistics or CDR data. Output will be in the 
format of CSV file and a graph (png) files
To illustrate, a simple csv file for cdr and mapping will be used and they are located in.
# Output Reports
## Data statistics 
Located in calculate_data_statistics(). In the data statistics, the result output (see the ****LINK******file here) provided will be:

1. Total Records (total_records): the total cdr usage data 
2. Total Days (total_days): the total days that have usage
3. Unique id (unique_id): the total unique ids of the data (it could be imei, imsi or another identifier given)
4. Unique imei (unique_imei): the total unique imeis (will be omitted if it is the unique id already)
5. Unique imsi (unique_imsi): the total unique imsis (will be omitted if it is the unique id already)
6. Unique location name (unique_location_name): the total unique latitude and longitude of the cdr data
7. Start date (start_date): the starting date of the data
8. End date (end_date): the end date of the data

##Daily and Monthly Statistics
Located in calculate_daily_statistics() and calculate_monthly_statistics (see the ****LINKdaily output and ****LINKmonthly output). Calculating some properties order by date first and then the type of call type and network type.

Each field in the daily statistics is by a particular date (or year and month for monthly statistics), call type and network type including:
1. Date (date): the dates that have cdr records
2. Call Type (call_type): the call type of the cdr data (VOICE, DATA, SMS)
3. Network Type (network_type): the network type of the cdr data (2G, 3G, 4G)
4. Total Records (total_records): the total records
5. Total Days (total_days): the total days
6. Unique id (unique_id): the total unique ids of the data (it could be imei, imsi or another identifier given)
7. Unique imei (unique_imei): the total unique imeis 
8. Unique imsi (unique_imsi): the total unique imsis
9  Unique location name (unique_location_name): the total unique latitude and longitude of the cdr data

## Zone population
atleast one administration level is needed to calculate zone population.
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
 
The result you get is the zone_base_aggregations_level_ADMIN{X}.csv and you can see the result output ****LINKhere\

which includes:
1. Administration Level x (admin{x}): Your input administration level x
2. Count Activities (count_activities): Total CDR Records that are in the administration level
3. Count Unique IDs (count_unique_ids): Total Unique Ids

and {geojson_file_name}_joined_ADMIN{X}.json (you can put this in visualization service such as https://kepler.gl)

## Summary Data
For the output summary date (****LINKhere). It contains overall statistics of the cdr data including:

1. Total records (total_records)
2. Total unique IDs (total_uids)
3. Total days (total_days): Counting from the first day that have cdr data to the last day that has the cdr data
4. Average usage per day (average_usage_per_day): average cdr records each day
5. Average daily voice (average_daily_voice): average cdr records of voice data each day
6. Average daily sms (average_daily_sms): average cdr records of sms data each day
7. Average daily unique cell id (average_daily_unique_cell_id): average cell id in the cdr data each day
8. Average district per day (average_district_per_day): average district in the cdr data each day

## Frequent Locations
Frequent locations (All-day and night) output is the most popular cell_id in which user make a call data in a day or a night.
The output here will be a table named {your_prefix}_frequent_location_thresholded and {your_prefix}_frequent_location_thresholded.
Example output


****LINK PHOTO of cropped table

# Output Graphs
## User Date Histogram
The output histogram (here) is the histogram of each number of days (x) with the corresponding number of unique ids of users who make a call for the particular number of days (y).
## Daily CDRs
The graph reports the daily usage of a user each day with minimum, maximum, average and total cdr
***PHOTOLINK
## Daily CDRs by call type
This graph reports the daily usage of a user each day by call type (multiple lines)
***PHOTOLINK
## Daily Unique Users
Daily unique uids are reported daily in the graph with statistics information (minimum, maximum, average and total)
***PHOTOLINK
## Daily Unique Locations
Daily unique locations (unique latitude and longitude) are reported daily in the graph with statistics information (minimum, maximum, average and total)
## Daily average CDRs
Daily average CDRs are the daily CDR per user in average with the average of them by days displayed on top
***PHOTOLINK
## Daily Unique Average Locations
This graphs represent daily average locations which are the daily CDR per user in average with the average of them by days being shown on the top of the graph
***PHOTOLINK
