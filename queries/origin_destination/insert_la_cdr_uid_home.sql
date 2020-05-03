INSERT OVERWRITE TABLE {provider_prefix}_la_cdr_uid_home
select uid, unique_location, tcount, trank, ppercent, latitude, longitude, admin1_id
from big6_frequent_locations where trank = 1