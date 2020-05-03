CREATE table {provider_prefix}_freq_with_acc_wsum_night as select uid, tcount,
trank, ppercent, unique_location, longitude, latitude , {admin}_id,
sum(ppercent) over (partition by uid order by trank asc)
as acc_wsum from {provider_prefix}_frequent_locations_night
order by uid, trank