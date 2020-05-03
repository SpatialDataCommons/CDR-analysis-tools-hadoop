create table {provider_prefix}_cdr_by_uid_trip_routing_array_apd
(uid string, route_arr ARRAY<ARRAY<string>>)
partitioned by (pdt string)
row format delimited fields terminated by '\t'
collection items terminated by ','
map keys terminated by '!'
lines terminated by '\n'
stored as ORC

