-- models/marts/fct_pr_events.sql
with pr as (
    select * from {{ ref('int_events') }}
    where event_type = 'PullRequestEvent'
)

select
    event_id,
    event_date,
    event_hour,
    is_weekend,
    actor_id,
    repo_id,
    repo_language,
    pr_number,
    pr_merged,
    pr_state,
    pr_additions,
    pr_deletions,
    pr_changed_files,
    pr_title
from pr