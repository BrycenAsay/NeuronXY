
UPDATE s3_bucket SET storage_class = cycr.transition_to

FROM lifecycle_rule cycr
WHERE s3_bucket.user_id = cycr.user_id
AND s3_bucket.bucket_id = cycr.bucket_id
AND s3_bucket.lifecycle_id = cycr.lifecycle_id
AND current_date::date - (SELECT creation_date::date FROM s3_bucket) = days_till_transition