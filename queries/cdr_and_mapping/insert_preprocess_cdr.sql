INSERT OVERWRITE TABLE  {provider_prefix}_preprocess
select {distinct} {arg} from {provider_prefix}_raw
