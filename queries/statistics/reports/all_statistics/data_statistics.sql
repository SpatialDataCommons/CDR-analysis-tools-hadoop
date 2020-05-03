select count(*) as total_records, count(distinct to_date(call_time)) as total_days,
count(distinct uid) as unique_id, {imei} {imsi} count(distinct cell_id) as unique_location_name,
min(to_date(call_time)) as start_date, max(to_date(call_time)) as end_date from {provider_prefix}_consolidate_data_all