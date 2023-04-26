select org_id, 
       ticket_id, 
       ntile(100) over (partition by org_id order by ticket_id asc) percentile
from {{ ref('mrt_zendesk_tickets') }} 