-- models/marts/github_hourly_activity.sql
with events as (
    select * from {{ ref('int_events') }}
)

select
    event_date,
    event_hour,
    day_of_week,
    count(*) as total_events,
    count(distinct actor_id) as unique_actors,
    countif(is_bot = false) as human_events,
    countif(is_bot = true) as bot_events,
    countif(event_type = 'PushEvent') as push_events,
    countif(event_type = 'PullRequestEvent') as pr_events,
    countif(event_type = 'WatchEvent') as watch_events,
    countif(event_type = 'ForkEvent') as fork_events
from events
group by 1, 2, 3
order by 1, 2, 3