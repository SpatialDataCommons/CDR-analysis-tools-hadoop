INSERT OVERWRITE TABLE  {provider_prefix}_la_cdr_all_with_ant_zone_by_uid  PARTITION (pdt)
select a1.uid, CreateTrajectoriesJICAWithZone (a1.uid, call_time, duration, a2.longitude, a2.latitude, a1.cell_id, a3.{target_admin}_id)
as arr, pdt from {provider_prefix}_consolidate_data_all a1 join {provider_prefix}_cell_tower_data_preprocess a2
on (a1.cell_id = a2.cell_id)
join {provider_prefix}_cell_tower_data_{target_admin} a3 on (a2.latitude = a3.latitude and a2.longitude = a3.longitude)
where to_date(pdt) = "{od_date}" group by a1.uid, pdt
