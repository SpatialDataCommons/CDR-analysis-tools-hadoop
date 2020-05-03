select to_date(call_time) as date, count(distinct latitude, longitude) as unique_locations
from {provider_prefix}_consolidate_data_all group by to_date(call_time) order by date