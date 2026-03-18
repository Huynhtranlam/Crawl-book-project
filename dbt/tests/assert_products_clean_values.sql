select *
from {{ source('stream_processor', 'products_clean') }}
where trim(product_id) = ''
   or trim(title) = ''
   or trim(currency) = ''
   or crawled_at > current_timestamp
   or (price is not null and price < 0)
