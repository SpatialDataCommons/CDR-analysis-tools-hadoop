CREATE table {provider_prefix}_freq_with_acc_wsum_night as select uid, cell_id, tcount,
trank, ppercent, longitude, latitude , {admin}_id,
sum(ppercent) over (partition by uid, cell_id order by trank asc)
as acc_wsum from {provider_prefix}_frequent_location_night
order by uid, cell_id, trank