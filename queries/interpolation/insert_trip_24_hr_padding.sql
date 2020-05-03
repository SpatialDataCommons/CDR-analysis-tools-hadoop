INSERT OVERWRITE TABLE  {provider_prefix}_cdr_by_uid_trip_organized_array_apd  PARTITION(pdt)
select uid, f_organizearray(uid,arr) as arr,pdt
from {provider_prefix}_cdr_by_uid_trip where size(arr) > 0
