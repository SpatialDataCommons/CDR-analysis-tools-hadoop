select count(*) as count_unique_locations from (select distinct latitude, longitude
from {provider_prefix}_consolidate_data_all) td