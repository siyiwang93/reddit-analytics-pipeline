-- models/marts/github_trending_repos.sql
with repo_summary as (
    select * from {{ ref('github_repo_summary') }}
)

select
    repo_id,
    repo_name,
    repo_owner,
    repo_language,
    total_stars,
    total_forks,
    total_pushes,
    total_prs,
    total_issues,
    unique_contributors,
    total_commits,
    pr_merge_rate,
    -- Trending score weighted by stars, forks and contributors
    (total_stars * 3) + (total_forks * 2) + unique_contributors as trending_score
from repo_summary
where total_stars > 0
order by trending_score desc