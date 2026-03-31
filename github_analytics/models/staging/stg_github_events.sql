-- models/staging/stg_github_events.sql
-- Light cleaning on raw github_events table
-- No aggregation, no business logic, just renaming and type casting

with source as (
    select * from {{ source('github_analytics', 'github_events') }}
),

staged as (
    select
        -- Event info
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

        -- Actor info
        actor_id,
        actor_login,
        -- Flag bots
        actor_login like '%[bot]%' as is_bot,

        -- Org info
        org_id,
        org_login,

        -- Repo info
        repo_id,
        repo_name,
        repo_owner,
        repo_short_name,
        repo_language,

        -- Push fields
        push_size,
        push_distinct_size,
        push_ref,

        -- PR fields
        pr_number,
        pr_merged,
        pr_state,
        pr_additions,
        pr_deletions,
        pr_changed_files,
        pr_title,

        -- Issue fields
        issue_number,
        issue_state,
        issue_title,
        issue_comments,

        -- Fork fields
        forkee_name,
        forkee_language,
        forkee_stars,
        forkee_forks,
        forkee_description,

        -- Action (WatchEvent etc)
        action

    from source
)

select * from staged