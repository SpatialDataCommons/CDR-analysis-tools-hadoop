CREATE TABLE {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od
(uid string, home_site_id string,home_zone string, arr ARRAY<ARRAY<string>>)
PARTITIONED BY (pdt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY '!'
LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE

