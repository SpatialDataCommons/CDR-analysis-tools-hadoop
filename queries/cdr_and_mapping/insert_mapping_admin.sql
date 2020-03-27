INSERT OVERWRITE TABLE  {provider_prefix}_cell_tower_data_{admin}
select row_number() OVER () - 1 as rowidx, {admin}, latitude, longitude
from {provider_prefix}_cell_tower_data_preprocess where translate({admin},'  ',' ') != ''
{check_lat_lng} group by {admin}, latitude, longitude order by rowidx