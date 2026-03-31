-- models/marts/fct_push_events.sql
with push as (
    select * from {{ ref('int_events') }}
    where event_type = 'PushEvent'
)

select
    event_id,
    event_date,
    event_hour,
    is_weekend,
    actor_id,
    repo_id,
    push_size,
    push_distinct_size,
    push_ref
from push
