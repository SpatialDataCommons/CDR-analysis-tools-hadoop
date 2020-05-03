insert overwrite local directory '/tmp/hive/csv_interpolation'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
select CONCAT_WS(',',uid,trip_seq, mobilitytype, mode, totaldistance, totaltime, starttime,endtime, totalpoints, regexp_replace(m,'\\\|',','))
from (select uid ,m[1] as trip_seq,m[2] as mobilitytype,m[3] as mode,m[4] totaldistance ,m[5] as
totaltime,m[6] as starttime,m[7] as endtime,m[8] as totalpoints,m[9] as pointlist
from (select * from {provider_prefix}_cdr_by_uid_trip_routing_array_apd where size(route_arr)>1) t1 LATERAL
VIEW explode(t1.route_arr) myTable1 AS m) t1 LATERAL VIEW explode(split(t1.pointlist,'\;')) myTable1 AS m




