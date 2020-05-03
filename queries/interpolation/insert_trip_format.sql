INSERT OVERWRITE TABLE {provider_prefix}_cdr_by_uid_trip  PARTITION(pdt)
select uid, TripSegmentationCDR(arr,uid) as arr,pdt from {provider_prefix}_cdr_by_uid
