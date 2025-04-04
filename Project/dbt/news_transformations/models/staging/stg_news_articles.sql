select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_ar') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_de') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_en') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_es') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_fr') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_he') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_it') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_nl') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_no') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_pt') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_ru') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_sv') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_ud') }}

union all

select
  article_id,
  title,
  description,
  publishedAt,
  source.name as source_name,
  language
from {{ source('news_data', 'extract_news_zh') }}