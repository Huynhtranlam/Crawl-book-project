select *
from {{ ref('dim_products') }}
where (product_price is not null and product_price < 0)
   or (product_rating is not null and (product_rating < 0 or product_rating > 5))
   or (stock_quantity is not null and stock_quantity < 0)
   or (
        product_price is not null
        and (
            (product_price < 25 and price_band <> 'low')
            or (product_price >= 25 and product_price < 75 and price_band <> 'mid')
            or (product_price >= 75 and price_band <> 'high')
        )
    )
