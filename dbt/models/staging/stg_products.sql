select
    product_id,
    trim(title) as product_title,
    price as product_price,
    upper(currency) as currency_code,
    product_url,
    image_url,
    source as source_name,
    crawled_at,
    raw_payload -> 'raw' ->> 'category' as category_name,
    raw_payload -> 'raw' ->> 'brand' as brand_name,
    cast(raw_payload -> 'raw' ->> 'rating' as numeric) as product_rating,
    cast(raw_payload -> 'raw' ->> 'stock' as integer) as stock_quantity,
    raw_payload -> 'raw' ->> 'availabilityStatus' as availability_status,
    raw_payload -> 'raw' ->> 'description' as product_description
from {{ source('stream_processor', 'products_clean') }}
