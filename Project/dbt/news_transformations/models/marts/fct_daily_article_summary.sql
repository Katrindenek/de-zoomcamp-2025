select
  date(publishedAt) as article_date,
  language,
  source_name,
  count(*) as article_count
from {{ ref('stg_news_articles') }}
group by 1, 2, 3
order by 1 desc