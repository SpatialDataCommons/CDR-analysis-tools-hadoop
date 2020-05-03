INSERT INTO TABLE {provider_prefix}_frequent_locations SELECT a1.uid, a2.cell_id,
count(a1.uid) as tcount, ROW_NUMBER() OVER(PARTITION BY a1.uid, a2.cell_id order by count(a1.uid) DESC) as rank,
count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid, a2.cell_id) * 100 as percentage
, a2.longitude, a2.latitude, a3.{admin_params} from {provider_prefix}_consolidate_data_all a1
JOIN {provider_prefix}_cell_tower_data_preprocess a2  ON(a1.cell_id = a2.cell_id)
JOIN {provider_prefix}_cell_tower_data_{admin} a3 on(a2.latitude = a3.latitude and a2.longitude = a3.longitude)
group by a1.uid, a2.latitude,  a2.longitude , a2.cell_id, a3.{admin_params} order by a1.uid, rank ASC
