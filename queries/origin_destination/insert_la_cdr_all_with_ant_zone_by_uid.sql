INSERT OVERWRITE TABLE  {provider_prefix}_la_cdr_all_with_ant_zone_by_uid  PARTITION (pdt)
select a1.uid, CreateTrajectoriesJICAWithZone (a1.uid, call_time, call_duration, a2.longitude, a2.latitude, a1.cell_id, a2.admin1)
as arr, pdt from {provider_prefix}_consolidate_data_all a1 join {provider_prefix}_cell_tower_data_preprocess a2
on (a1.cell_id = a2.cell_id) where to_date(pdt) = '2016-05-01' group by a1.uid, pdt