select
  date(a.published_at) as article_date,
  l.language_name,
  a.source__name,
  count(*) as article_count
from {{ ref('stg_news_articles') }} a
left join {{ ref('language_codes') }} l
  on a.language = l.language_code
group by 1, 2, 3
order by 1 desc