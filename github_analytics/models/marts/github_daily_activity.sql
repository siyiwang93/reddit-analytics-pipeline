-- models/marts/github_daily_activity.sql
with events as (
    select * from {{ ref('int_events') }}
)

select
    event_date,
    event_type,
    -- Event counts
    count(*) as total_events,
    count(distinct actor_id) as unique_actors,
    count(distinct repo_id) as unique_repos,

    -- Bot vs human
    countif(is_bot = true) as bot_events,
    countif(is_bot = false) as human_events

from events
group by 1, 2