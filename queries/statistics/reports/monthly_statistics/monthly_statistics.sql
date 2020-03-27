SELECT YEAR(call_time) as year, MONTH(call_time) as month  , 'ALL' as call_type, 'ALL' as network_type, COUNT(*) as total_records,
COUNT(DISTINCT TO_DATE(call_time)) as total_days, COUNT(DISTINCT uid) as unique_id, {imei} {imsi}
COUNT(DISTINCT cell_id) as unique_location_name FROM {provider_prefix}_consolidate_data_all where
(year(pdt) between {start_year} and {end_year}) and (MONTH(pdt) between {start_month} and {end_month})
GROUP BY YEAR(call_time), MONTH(call_time)
UNION
SELECT YEAR(call_time) as year, MONTH(call_time) as month, call_type, 'ALL' as network_type, COUNT(*) as total_records,
COUNT(DISTINCT TO_DATE(call_time)) as total_days, COUNT(DISTINCT uid) as unique_id, COUNT(DISTINCT imei) as unique_imei,
COUNT(DISTINCT imsi) unique_imsi, COUNT(DISTINCT cell_id) as unique_location_name FROM {provider_prefix}_consolidate_data_all
where (year(pdt) between {start_year} and {end_year}) and (MONTH(pdt) between {start_month} and {end_month})
GROUP BY YEAR(call_time), MONTH(call_time), call_type
UNION
SELECT YEAR(call_time) as year, MONTH(call_time) as month, 'ALL' as call_type,  network_type, COUNT(*) as total_records,
COUNT(DISTINCT TO_DATE(call_time)) as total_days, COUNT(DISTINCT uid) as unique_id, {imei} {imsi}
COUNT(DISTINCT cell_id) as unique_location_name FROM {provider_prefix}_consolidate_data_all where (year(pdt) between {start_year} and {end_year})
and (MONTH(pdt) between {start_month} and {end_month}) GROUP BY YEAR(call_time), MONTH(call_time), network_type
UNION
SELECT YEAR(call_time) as year, MONTH(call_time) as month , call_type, network_type, COUNT(*) as total_records,
COUNT(DISTINCT TO_DATE(call_time)) as total_days, COUNT(DISTINCT uid) as unique_id, {imei} {imsi}
COUNT(DISTINCT cell_id) as unique_location_name FROM {provider_prefix}_consolidate_data_all
where (year(pdt) between {start_year} and {end_year}) and (MONTH(pdt) between {start_month} and {end_month})
GROUP BY YEAR(call_time), MONTH(call_time), call_type, network_type ORDER BY year ASC, month ASC, call_type ASC, network_type DESC
