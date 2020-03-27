select count(*)/{total_days} as average_daily_sms
from {provider_prefix}_consolidate_data_all
where call_type = 'SMS'