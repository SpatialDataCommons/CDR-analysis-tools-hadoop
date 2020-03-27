select count(*) as total_uids from
(select distinct uid from {provider_prefix}_consolidate_data_all) td