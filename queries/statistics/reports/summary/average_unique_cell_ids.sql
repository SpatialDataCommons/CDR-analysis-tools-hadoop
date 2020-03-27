select sum(td.count)/{total_days} as average_daily_unique_cell_id from
(select count(distinct cell_id) as count from {provider_prefix}_consolidate_data_all
group by to_date(call_time)) td
