select
  date(published_at) as article_date,
  language,
  source__name,
  count(*) as article_count
from {{ ref('stg_news_articles') }}
group by 1, 2, 3
order by 1 desc