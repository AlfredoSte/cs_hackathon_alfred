WITH stg_zendesk_tickets AS
(Select `organization_id`,
  `requester_id`,
  `is_public`,
  `description`,
  `follower_ids`,
  `submitter_id`,
  `generated_timestamp`,
  `id`,
  `group_id`,
  `type`,
  `collaborator_ids`,
  `tags`,
  `has_incidents`,
  `created_at`,
  `raw_subject`,
  `status`,
  `updated_at`,
  `custom_fields`,
  `url`,
  `priority`,
  `assignee_id`,
  `subject`,
  JSON_EXTRACT_SCALAR(satisfaction_rating, '$.reason') AS satisfaction_rating_reason,
  JSON_EXTRACT_SCALAR(satisfaction_rating, '$.score') AS satisfaction_rating_score
                     FROM {{ source('vdemo_public_cshackathoneumultiregion_main',
                     'Integrations_Y42_Analytics_Integrations_src_zendesk_tickets') }}
),

array_fix AS
(SELECT
id,
SPLIT(REPLACE(custom_fields,"},","}_split_"),"_split_") AS custom_fields
FROM
stg_zendesk_tickets),

unnested_data AS
(SELECT
id AS ticket_id, 
REPLACE(REPLACE(custom_fields_unnested,"]",""),"[","") AS custom_fields_unnested
FROM array_fix,
UNNEST(custom_fields) AS custom_fields_unnested),

zendesk_custom_fields_unnested AS
(SELECT
ticket_id,
CAST(REPLACE(JSON_EXTRACT(custom_fields_unnested, '$.id'),'"','') AS STRING)  AS custom_field_id,
CAST(REPLACE(JSON_EXTRACT(custom_fields_unnested, '$.value'),'"','') AS STRING)  AS custom_field_value
FROM unnested_data),

custom_fields_unnested AS 
(SELECT
ticket_id, custom_field_value,
CASE 
WHEN custom_field_id = '4418230165905' THEN 'internal_assignee'
WHEN custom_field_id = '1900005211553' THEN 'y42_product'
WHEN custom_field_id = '4414123767441' THEN 'product_ticket_type'
WHEN custom_field_id = '4416056972561' THEN 'organization_slug'
WHEN custom_field_id = '4419817009169' THEN 'forwarded_to_product'
WHEN custom_field_id = '4432899603345' THEN 'ticket_category'
WHEN custom_field_id = '4414114902929' THEN 'data_source'
WHEN custom_field_id = '4416597995537' THEN 'clickup_link'
WHEN custom_field_id = '7120181882653' THEN 'app_version'
WHEN custom_field_id = '5284790255121' THEN 'integration_name'
ELSE 'delete'
END AS custom_field_name
FROM zendesk_custom_fields_unnested )

SELECT *
FROM custom_fields_unnested
pivot (string_agg(custom_field_value) for custom_field_name in ('internal_assignee'
,'y42_product'
,'product_ticket_type'
,'organization_slug'
,'forwarded_to_product'
,'ticket_category'
,'data_source'
,'clickup_link'
,'app_version'
,'integration_name')) t1
JOIN stg_zendesk_tickets t2
ON id=ticket_id
JOIN (Select `domain_names`, `name` AS organization_name, id AS org_id
                     FROM {{ source('vdemo_public_cshackathoneumultiregion_main',
                     'Integrations_Y42_Analytics_Integrations_src_zendesk_organizations') }}) t3
ON t2.organization_id=t3.org_id