INSERT INTO TABLE  {provider_prefix}_consolidate_data_all
PARTITION (pdt) select {arg_con}, to_date(call_time) as pdt
from {provider_prefix}_preprocess

