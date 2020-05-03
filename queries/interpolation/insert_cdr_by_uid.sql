INSERT OVERWRITE TABLE {provider_prefix}_cdr_by_uid  PARTITION(pdt)
select uid as uid, CreateTrajectoriesCDR(time, duration,cell_id,longitude,latitude) as arr, pdate as pdt
from cdr_test_interpolation group by pdate, uid
having count(*) <= {max_size_cdr_by_uid}