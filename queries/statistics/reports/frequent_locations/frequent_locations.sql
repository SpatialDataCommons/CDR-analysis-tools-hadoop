INSERT INTO TABLE {provider_prefix}_frequent_locations SELECT a1.uid,
count(a1.uid) as tcount, ROW_NUMBER() OVER(PARTITION BY a1.uid order by count(a1.uid) DESC) as rank,
count(a1.uid)/SUM(count(a1.uid)) OVER(partition by a1.uid) * 100 as ppercent
, concat(a1.latitude, ' : ', a1.longitude) as unique_location, a1.latitude, a1.longitude,
a2.{admin_params} from {provider_prefix}_consolidate_data_all a1
JOIN {provider_prefix}_cell_tower_data_{admin} a2 on(a1.latitude = a2.latitude and a1.longitude = a2.longitude)
group by a1.uid, concat(a2.latitude, ' : ', a2.longitude), a1.latitude, a1.longitude, a2.{admin_params}
order by a1.uid, rank