insert overwrite table {provider_prefix}_cdr_by_uid_trip_realloc_array_apd partition (pdt)
select uid,f_reallocation(uid,arr,pdt, "{poi}"),pdt
from {provider_prefix}_cdr_by_uid_trip_organized_array_apd