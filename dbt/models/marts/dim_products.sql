select
    product_id,
    product_title,
    category_name,
    brand_name,
    product_price,
    currency_code,
    stock_quantity,
    product_rating,
    availability_status,
    case
        when stock_quantity > 0 then true
        else false
    end as is_in_stock,
    case
        when product_price < 25 then 'low'
        when product_price < 75 then 'mid'
        else 'high'
    end as price_band,
    source_name,
    crawled_at
from {{ ref('stg_products') }}
