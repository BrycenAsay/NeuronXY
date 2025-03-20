
UPDATE cortex_node SET storage_class = cyct.transition_to

FROM cortex

INNER JOIN lifecycle_rule cycr
ON cycr.user_id = s3.user_id
AND cycr.bucket_id = s3.bucket_id
AND cycr.lifecycle_id = s3.lifecycle_id

INNER JOIN lifecycle_transition cyct
ON cycr.lifecycle_id = cyct.lifecycle_id

WHERE cortex_node.user_id = s3.user_id
AND cortex_node.bucket_id = s3.bucket_id
AND current_date::date - (SELECT creation_date::date FROM cortex_node) = cyct.days_till_transition
AND cycr.rule_enabled = True