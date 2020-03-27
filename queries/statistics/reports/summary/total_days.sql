select count(*) as total_days, min(dates) as start_date, max(dates)
as end_date from (select  to_date(call_time) as dates from
{provider_prefix}_consolidate_data_all group by to_date(call_time)) td