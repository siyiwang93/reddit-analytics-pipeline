-- models/marts/dim_repos.sql
with repos as (
    select * from {{ ref('int_events') }}
)

select
    repo_id,        -- immutable, never changes
    repo_name,      -- can change but use latest
    repo_owner,     -- can change but use latest
    repo_short_name -- can change but use latest
from repos
where repo_id is not null
qualify row_number() over (partition by repo_id order by created_at desc) = 1