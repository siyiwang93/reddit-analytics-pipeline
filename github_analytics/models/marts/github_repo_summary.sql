with events as (
    select * from {{ ref('int_events') }}
),

push as (
    select * from {{ ref('fct_push_events') }}
),

pr as (
    select * from {{ ref('fct_pr_events') }}
),

repo_events as (
    select
        repo_id,
        max(repo_name) as repo_name,
        max(repo_owner) as repo_owner,
        max(repo_short_name) as repo_short_name,
        max(repo_language) as repo_language,
        count(*) as total_events,
        count(distinct actor_id) as unique_contributors,
        countif(event_type = 'WatchEvent') as total_stars,
        countif(event_type = 'ForkEvent') as total_forks,
        countif(event_type = 'PushEvent') as total_pushes,
        countif(event_type = 'PullRequestEvent') as total_prs,
        countif(event_type = 'IssuesEvent') as total_issues,
        min(event_date) as first_seen,
        max(event_date) as last_seen
    from events
    group by repo_id
),

repo_push as (
    select
        repo_id,
        sum(push_size) as total_commits,
        avg(push_size) as avg_commits_per_push,
        max(push_size) as max_push_size
    from push
    group by repo_id
),

repo_pr as (
    select
        repo_id,
        countif(pr_merged = true) as merged_prs,
        countif(pr_merged = false) as unmerged_prs,
        avg(pr_additions) as avg_pr_additions,
        avg(pr_deletions) as avg_pr_deletions
    from pr
    group by repo_id
)

select
    r.*,
    p.total_commits,
    p.avg_commits_per_push,
    p.max_push_size,
    pr.merged_prs,
    pr.unmerged_prs,
    safe_divide(pr.merged_prs, r.total_prs) as pr_merge_rate
from repo_events r
left join repo_push p using (repo_id)
left join repo_pr pr using (repo_id)