CREATE TABLE {provider_prefix}_raw ({arg_raw})
ROW FORMAT DELIMITED FIELDS TERMINATED BY "{field_delimiter}"
LINES TERMINATED BY '\n' STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="{cell_tower_header}")