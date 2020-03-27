SELECT to_date(call_time) as date, 'ALL' as call_type, 'ALL' as network_type, COUNT(*) as total_records,
COUNT(DISTINCT TO_DATE(call_time)) as total_days, COUNT(DISTINCT uid) as unique_id, {imei} {imsi}
COUNT(DISTINCT cell_id) as unique_location_name FROM {provider_prefix}_consolidate_data_all where to_date(pdt)
between to_date('{start_date}') and to_date('{end_date}') GROUP BY to_date(call_time)
UNION
SELECT to_date(call_time) as date, call_type, 'ALL' as network_type, COUNT(*) as total_records,
COUNT(DISTINCT TO_DATE(call_time)) as total_days, COUNT(DISTINCT uid) as unique_id, {imei} {imsi}
COUNT(DISTINCT cell_id) as unique_location_name FROM {provider_prefix}_consolidate_data_all where to_date(pdt)
between to_date('{start_date}') and to_date('{end_date}') GROUP BY to_date(call_time), call_type
UNION
SELECT to_date(call_time) as date, 'ALL' as call_type,  network_type, COUNT(*) as total_records,
COUNT(DISTINCT TO_DATE(call_time)) as total_days, COUNT(DISTINCT uid) as unique_id, {imei} {imsi}
COUNT(DISTINCT cell_id) as unique_location_name FROM {provider_prefix}_consolidate_data_all where to_date(pdt)
between to_date('{start_date}') and to_date('{end_date}') GROUP BY to_date(call_time), network_type
UNION
SELECT to_date(call_time) as date, call_type, network_type, COUNT(*) as total_records,
COUNT(DISTINCT TO_DATE(call_time)) as total_days, COUNT(DISTINCT uid) as unique_id, {imei} {imsi}
COUNT(DISTINCT cell_id) as unique_location_name FROM {provider_prefix}_consolidate_data_all where to_date(pdt)
between to_date('{start_date}') and to_date('{end_date}') GROUP BY to_date(call_time), call_type, network_type
ORDER BY date ASC, call_type ASC, network_type DESC
