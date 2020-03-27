CREATE TABLE {provider_prefix}_cell_tower_data_raw ({arg_raw}) ROW FORMAT DELIMITED
FIELDS TERMINATED BY "{field_delimiter}" LINES TERMINATED BY '\n' STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="{have_header}")