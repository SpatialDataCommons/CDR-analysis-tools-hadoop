CREATE TABLE {provider_prefix}_frequent_locations_night  (uid string, tcount int,trank int,ppercent double,
unique_location string, latitude string, longitude string, {admin_params})
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY '!' LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE