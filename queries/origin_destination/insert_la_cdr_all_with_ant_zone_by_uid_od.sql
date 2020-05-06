INSERT OVERWRITE TABLE {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od PARTITION (pdt)
select t1.uid,t2.site_id as home_site_id, t2.{target_unit}_id as home_zone,
TripOD(arr, t1.uid, t2.site_id, t2.{target_unit}_id, t2.LONGITUDE, t2.LATITUDE), pdt
from {provider_prefix}_la_cdr_all_with_ant_zone_by_uid t1
inner join {provider_prefix}_la_cdr_uid_home t2 on t1.uid = t2.uid
where size(arr) <= 500
