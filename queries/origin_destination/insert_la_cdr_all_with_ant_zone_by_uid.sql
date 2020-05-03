INSERT OVERWRITE TABLE  {provider_prefix}_la_cdr_all_with_ant_zone_by_uid  PARTITION (pdt)
select a1.uid, CreateTrajectoriesJICAWithZone
(a1.uid, call_time, duration, a1.longitude, a1.latitude, concat(a1.latitude, ' : ', a1.longitude), a2.{target_admin}_id)
as arr, pdt from {provider_prefix}_consolidate_data_all a1
join {provider_prefix}_cell_tower_data_{target_admin} a2 on (a1.latitude = a2.latitude and a1.longitude = a2.longitude)
where to_date(pdt) = "{od_date}" group by a1.uid, pdt