with recursive ordered as (
    select
        *,
        row_number() over (
            partition by symbol, interval
            order by close_time
        ) as row_num,
        lag(close_price) over (
            partition by symbol, interval
            order by close_time
        ) as previous_close,
        avg(close_price) over (
            partition by symbol, interval
            order by close_time
            rows between 19 preceding and current row
        ) as sma_20,
        avg(close_price) over (
            partition by symbol, interval
            order by close_time
            rows between 49 preceding and current row
        ) as sma_50,
        avg(volume) over (
            partition by symbol, interval
            order by close_time
            rows between 19 preceding and current row
        ) as volume_ma_20,
        max(high_price) over (
            partition by symbol, interval
            order by close_time
            rows between 19 preceding and current row
        ) as rolling_high_20,
        min(low_price) over (
            partition by symbol, interval
            order by close_time
            rows between 19 preceding and current row
        ) as rolling_low_20
    from {{ ref('stg_btc_klines') }}
),
ema as (
    select
        symbol,
        interval,
        row_num,
        close_price as ema_20,
        close_price as ema_50
    from ordered
    where row_num = 1

    union all

    select
        current_row.symbol,
        current_row.interval,
        current_row.row_num,
        (current_row.close_price * (2.0 / 21.0))
        + (previous_row.ema_20 * (1 - (2.0 / 21.0))) as ema_20,
        (current_row.close_price * (2.0 / 51.0))
        + (previous_row.ema_50 * (1 - (2.0 / 51.0))) as ema_50
    from ordered as current_row
    inner join ema as previous_row
        on current_row.symbol = previous_row.symbol
       and current_row.interval = previous_row.interval
       and current_row.row_num = previous_row.row_num + 1
),
enriched as (
    select
        ordered.event_id,
        ordered.source,
        ordered.symbol,
        ordered.interval,
        ordered.open_time as candle_time,
        ordered.open_price as open,
        ordered.high_price as high,
        ordered.low_price as low,
        ordered.close_price as close,
        ordered.volume,
        ordered.quote_asset_volume,
        ordered.trade_count,
        ordered.is_closed,
        ordered.ingest_time,
        ordered.sma_20,
        ordered.sma_50,
        ema.ema_20,
        ema.ema_50,
        ordered.volume_ma_20,
        ordered.rolling_high_20,
        ordered.rolling_low_20,
        case
            when ordered.open_price = 0 then null
            else ((ordered.high_price - ordered.low_price) / ordered.open_price) * 100
        end as candle_range_pct,
        greatest(
            ordered.high_price - ordered.low_price,
            abs(ordered.high_price - coalesce(ordered.previous_close, ordered.close_price)),
            abs(ordered.low_price - coalesce(ordered.previous_close, ordered.close_price))
        ) as true_range,
        greatest(ordered.close_price - coalesce(ordered.previous_close, ordered.close_price), 0) as gain,
        greatest(coalesce(ordered.previous_close, ordered.close_price) - ordered.close_price, 0) as loss,
        ordered.previous_close,
        ordered.close_time
    from ordered
    inner join ema
        on ordered.symbol = ema.symbol
       and ordered.interval = ema.interval
       and ordered.row_num = ema.row_num
),
signals as (
    select
        *,
        avg(true_range) over (
            partition by symbol, interval
            order by close_time
            rows between 13 preceding and current row
        ) as atr_14,
        avg(gain) over (
            partition by symbol, interval
            order by close_time
            rows between 13 preceding and current row
        ) as avg_gain_14,
        avg(loss) over (
            partition by symbol, interval
            order by close_time
            rows between 13 preceding and current row
        ) as avg_loss_14
    from enriched
)
select
    event_id,
    source,
    symbol,
    interval,
    candle_time,
    open,
    high,
    low,
    close,
    volume,
    quote_asset_volume,
    trade_count,
    is_closed,
    ingest_time,
    sma_20,
    sma_50,
    ema_20,
    ema_50,
    volume_ma_20,
    case
        when avg_loss_14 = 0 and avg_gain_14 > 0 then 100
        when avg_loss_14 = 0 then 50
        else 100 - (100 / (1 + (avg_gain_14 / nullif(avg_loss_14, 0))))
    end as rsi_14,
    atr_14,
    candle_range_pct,
    rolling_high_20,
    rolling_low_20,
    case
        when ema_20 > ema_50 and close > sma_20 then 'bullish'
        when ema_20 < ema_50 and close < sma_20 then 'bearish'
        else 'ranging'
    end as trend_regime
from signals
