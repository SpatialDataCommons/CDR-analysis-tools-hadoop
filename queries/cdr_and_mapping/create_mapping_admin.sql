CREATE TABLE IF NOT EXISTS {provider_prefix}_cell_tower_data_{admin}
({admin}_id string, {admin}_name string, latitude string, longitude string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS SEQUENCEFILE