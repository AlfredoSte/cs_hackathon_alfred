SELECT 
  `id` AS ticket_metric_id,
  t1.`ticket_id`,
  CAST(JSON_EXTRACT_SCALAR(`agent_wait_time_in_minutes`, '$.calendar') AS INT64) AS agent_wait_time_in_minutes,
  `assignee_stations`,
  `created_at`,
  CAST(JSON_EXTRACT_SCALAR(`first_resolution_time_in_minutes`, '$.calendar') AS INT64) AS first_resolution_time_in_minutes,
   CAST(JSON_EXTRACT_SCALAR(`full_resolution_time_in_minutes`, '$.calendar') AS INT64) AS full_resolution_time_in_minutes,
  `group_stations`,
  `latest_comment_added_at`,
  CAST(JSON_EXTRACT_SCALAR(`on_hold_time_in_minutes`, '$.calendar') AS INT64) AS on_hold_time_in_minutes,
  `reopens`,
  `replies`,
  CAST(JSON_EXTRACT_SCALAR(`reply_time_in_minutes`, '$.calendar') AS INT64) AS reply_time_in_minutes,
  `requester_updated_at`,
  CAST(JSON_EXTRACT_SCALAR(`requester_wait_time_in_minutes`, '$.calendar') AS INT64) AS requester_wait_time_in_minutes,
  `status_updated_at`,
  `updated_at`,
  `url`,
  `initially_assigned_at`,
  `assigned_at`,
  `solved_at`,
  `assignee_updated_at`,
  first_status_change,
  TIMESTAMP_DIFF(CAST(first_status_change AS TIMESTAMP), CAST(created_at AS TIMESTAMP), MINUTE) AS time_to_react_in_minutes
                     FROM {{ source('vdemo_public_cshackathoneumultiregion_main',
                     'Integrations_Y42_Analytics_Integrations_src_zendesk_ticket_metrics') }} t1
                     JOIN (
                     SELECT MIN(`created_at`) AS first_status_change, `ticket_id`
                     FROM {{ source('vdemo_public_cshackathoneumultiregion_main',
                     'Integrations_Y42_Analytics_Integrations_src_zendesk_ticket_audits') }}
                     WHERE REGEXP_CONTAINS(events, 'Change')
                     GROUP BY 2
                     ) t2
ON t1.ticket_id = t2.ticket_id

