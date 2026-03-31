-- models/marts/fct_events.sql
-- Base fact table with common fields across all event types
with events as (
    select * from {{ ref('int_events') }}
)

select
    event_id,
    event_type,
    created_at,
    event_date,
    event_year,
    event_month,
    event_day,
    event_hour,
    day_of_week,
    is_weekend,
    actor_id,
    repo_id,
    org_id,
    org_login 
from events
