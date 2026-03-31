-- models/marts/dim_actors.sql
with actors as (
    select * from {{ ref('int_events') }}
)


select
        actor_id,
        actor_login,
        actor_login like '%[bot]%' as is_bot
from actors
where actor_id is not null
qualify row_number() over (partition by actor_id order by created_at desc) = 1

