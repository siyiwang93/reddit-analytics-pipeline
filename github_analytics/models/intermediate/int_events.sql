-- models/intermediate/int_events.sql
-- Deduplicated events, single source of truth for all downstream models

with staged as (
    select * from {{ ref('stg_github_events') }}
),

deduped as (
    select *
    from staged
    qualify row_number() over (partition by event_id order by created_at) = 1
)

select * from deduped