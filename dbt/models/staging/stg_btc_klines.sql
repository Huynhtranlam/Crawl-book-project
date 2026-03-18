select
    event_id,
    source,
    upper(symbol) as symbol,
    interval,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    quote_asset_volume,
    trade_count,
    is_closed,
    ingest_time,
    raw_payload
from {{ source('market_data', 'raw_btc_kline_events') }}
