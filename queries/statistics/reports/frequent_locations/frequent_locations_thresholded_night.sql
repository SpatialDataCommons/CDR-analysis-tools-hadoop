create table {provider_prefix}_frequent_location_thresholded_night as select td3.uid as uid, td3.cell_id as cell_id, td3.tcount
as tcount, td3.trank as trank, td3.ppercent as ppercent, td3.longitude as longitude, td3.latitude as latitude,
td3.{admin}_id as {admin}_id, td3.acc_wsum as acc_wsum, td3.min_acc_wsum as min_acc_wsum from
(select a1.uid as uid, a1.cell_id as cell_id, a1.tcount as tcount, a1.trank as trank,
a1.ppercent as ppercent, a1.longitude as longitude, a1.latitude as latitude,
a1.{admin}_id as {admin}_id, a1.acc_wsum as acc_wsum, td2.min_acc_wsum as min_acc_wsum
from {provider_prefix}_freq_with_acc_wsum_night a1
join (select td.uid as uid, td.cell_id as cell_id , min(td.acc_wsum) as min_acc_wsum from (
select uid, cell_id, acc_wsum from  {provider_prefix}_freq_with_acc_wsum_night
where acc_wsum >= {threshold} group by uid, cell_id, acc_wsum)td group by td.uid, td.cell_id) td2
on (a1.uid = td2.uid and a1.cell_id = td2.cell_id)) td3 where acc_wsum <= min_acc_wsum