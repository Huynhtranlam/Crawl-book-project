select *
from {{ source('market_data', 'raw_btc_ticker_events') }}
where trim(symbol) = ''
   or last_price <= 0
   or volume_24h < 0
   or quote_volume_24h < 0
   or trade_count_24h < 0
   or high_price_24h < low_price_24h
