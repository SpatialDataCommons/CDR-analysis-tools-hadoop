CREATE TABLE {provider_prefix}_frequent_location_night  (uid string, cell_id string,tcount int,trank int,ppercent double,
LONGITUDE string, LATITUDE string, {admin_params})
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY '!' LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE