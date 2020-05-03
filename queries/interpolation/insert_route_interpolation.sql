insert overwrite table {provider_prefix}_cdr_by_uid_trip_routing_array_apd partition (pdt)
select uid,f_routing(uid,arr,"{osm}","{voronoi}"), pdt
from {provider_prefix}_cdr_by_uid_trip_realloc_array_apd
where (size(arr)>0 and  size(arr)<={max_size_interpolation})
