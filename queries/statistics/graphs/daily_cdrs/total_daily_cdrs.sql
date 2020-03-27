 select to_date(call_time) as date, count(*) as total_records
 from {provider_prefix}_consolidate_data_all group by to_date(call_time)
 order by date