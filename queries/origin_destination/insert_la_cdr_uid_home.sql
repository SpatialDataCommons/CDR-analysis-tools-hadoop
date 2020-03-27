INSERT OVERWRITE TABLE  {provider_prefix}_la_cdr_uid_home
SELECT * FROM {provider_prefix}_frequent_location where trank = 1