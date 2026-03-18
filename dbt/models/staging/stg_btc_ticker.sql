select
    event_id,
    source,
    upper(symbol) as symbol,
    event_time,
    ingest_time,
    last_price,
    price_change_24h,
    price_change_pct_24h,
    volume_24h,
    quote_volume_24h,
    open_price_24h,
    high_price_24h,
    low_price_24h,
    trade_count_24h,
    raw_payload
from {{ source('market_data', 'raw_btc_ticker_events') }}
