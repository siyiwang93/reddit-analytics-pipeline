-- models/marts/github_platform_metrics.sql
-- Platform health metrics for GitHub analytics dashboard

with events as (
    select * from {{ ref('int_events') }}
),

-- 1. Daily Active Users
daily_active_users as (
    select
        event_date,
        is_weekend,
        count(distinct actor_id) as total_active_users,
        countif(is_bot = false) as human_active_users,
        countif(is_bot = true) as bot_active_users,
        safe_divide(
            countif(is_bot = true),
            count(distinct actor_id)
        ) as bot_ratio
    from events
    group by 1,2
),

-- 2. Developer Engagement
developer_engagement as (
    select
        event_date,
        is_weekend,
        count(*) as total_events,
        countif(event_type = 'PushEvent') as total_pushes,
        countif(event_type = 'PullRequestEvent') as total_prs,
        countif(event_type = 'IssuesEvent') as total_issues,
        countif(event_type = 'WatchEvent') as total_stars,
        countif(event_type = 'ForkEvent') as total_forks,
        countif(event_type = 'CreateEvent') as total_new_repos,
        count(distinct repo_id) as unique_repos
    from events
    group by 1,2
)

select
    d.*,
    e.total_events,
    e.total_pushes,
    e.total_prs,
    e.total_issues,
    e.total_stars,
    e.total_forks,
    e.total_new_repos,
    e.unique_repos
from daily_active_users d
left join developer_engagement e using (event_date, is_weekend)