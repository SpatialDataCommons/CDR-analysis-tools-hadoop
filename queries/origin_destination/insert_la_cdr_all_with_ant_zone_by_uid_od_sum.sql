INSERT OVERWRITE TABLE {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum PARTITION(pdt)  
select arr[2] as origin, arr[3]  as destination, count(*) as tcount, count(distinct uid) as tusercount, pdt
from {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail  where ((arr[2] != '-1' and arr[3] != '-1' ) )
group by pdt,arr[2],arr[3]