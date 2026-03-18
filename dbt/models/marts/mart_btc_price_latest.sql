with candles as (
    select
        *,
        avg(atr_14) over (
            partition by symbol, interval
            order by candle_time
            rows between 19 preceding and current row
        ) as atr_rolling_avg_20,
        row_number() over (
            partition by symbol, interval
            order by candle_time desc
        ) as reverse_row_num
    from {{ ref('mart_btc_ohlcv') }}
),
latest_candles as (
    select *
    from candles
    where reverse_row_num = 1
),
latest_ticker as (
    select
        *,
        row_number() over (partition by symbol order by event_time desc) as reverse_row_num
    from {{ ref('stg_btc_ticker') }}
),
enriched as (
    select
        latest_candles.*,
        previous_5m.close as close_5m_ago,
        previous_15m.close as close_15m_ago,
        previous_1h.close as close_1h_ago,
        previous_4h.close as close_4h_ago,
        previous_1d.close as close_1d_ago,
        previous_1w.close as close_1w_ago
    from latest_candles
    left join lateral (
        select history.close
        from {{ ref('mart_btc_ohlcv') }} as history
        where history.symbol = latest_candles.symbol
          and history.interval = latest_candles.interval
          and history.candle_time <= latest_candles.candle_time - interval '5 minutes'
        order by history.candle_time desc
        limit 1
    ) as previous_5m on true
    left join lateral (
        select history.close
        from {{ ref('mart_btc_ohlcv') }} as history
        where history.symbol = latest_candles.symbol
          and history.interval = latest_candles.interval
          and history.candle_time <= latest_candles.candle_time - interval '15 minutes'
        order by history.candle_time desc
        limit 1
    ) as previous_15m on true
    left join lateral (
        select history.close
        from {{ ref('mart_btc_ohlcv') }} as history
        where history.symbol = latest_candles.symbol
          and history.interval = latest_candles.interval
          and history.candle_time <= latest_candles.candle_time - interval '1 hour'
        order by history.candle_time desc
        limit 1
    ) as previous_1h on true
    left join lateral (
        select history.close
        from {{ ref('mart_btc_ohlcv') }} as history
        where history.symbol = latest_candles.symbol
          and history.interval = latest_candles.interval
          and history.candle_time <= latest_candles.candle_time - interval '4 hours'
        order by history.candle_time desc
        limit 1
    ) as previous_4h on true
    left join lateral (
        select history.close
        from {{ ref('mart_btc_ohlcv') }} as history
        where history.symbol = latest_candles.symbol
          and history.interval = latest_candles.interval
          and history.candle_time <= latest_candles.candle_time - interval '1 day'
        order by history.candle_time desc
        limit 1
    ) as previous_1d on true
    left join lateral (
        select history.close
        from {{ ref('mart_btc_ohlcv') }} as history
        where history.symbol = latest_candles.symbol
          and history.interval = latest_candles.interval
          and history.candle_time <= latest_candles.candle_time - interval '1 week'
        order by history.candle_time desc
        limit 1
    ) as previous_1w on true
)
select
    enriched.symbol,
    enriched.interval,
    enriched.candle_time as as_of_time,
    enriched.close as last_price,
    latest_ticker.price_change_pct_24h as price_change_pct_24h_exchange,
    case when enriched.close_5m_ago is null or enriched.close_5m_ago = 0 then null else ((enriched.close - enriched.close_5m_ago) / enriched.close_5m_ago) * 100 end as price_change_pct_5m,
    case when enriched.close_15m_ago is null or enriched.close_15m_ago = 0 then null else ((enriched.close - enriched.close_15m_ago) / enriched.close_15m_ago) * 100 end as price_change_pct_15m,
    case when enriched.close_1h_ago is null or enriched.close_1h_ago = 0 then null else ((enriched.close - enriched.close_1h_ago) / enriched.close_1h_ago) * 100 end as price_change_pct_1h,
    case when enriched.close_4h_ago is null or enriched.close_4h_ago = 0 then null else ((enriched.close - enriched.close_4h_ago) / enriched.close_4h_ago) * 100 end as price_change_pct_4h,
    case when enriched.close_1d_ago is null or enriched.close_1d_ago = 0 then null else ((enriched.close - enriched.close_1d_ago) / enriched.close_1d_ago) * 100 end as price_change_pct_24h,
    case when enriched.close_1w_ago is null or enriched.close_1w_ago = 0 then null else ((enriched.close - enriched.close_1w_ago) / enriched.close_1w_ago) * 100 end as price_change_pct_1w,
    latest_ticker.volume_24h,
    latest_ticker.quote_volume_24h,
    case when enriched.close = 0 then null else (enriched.atr_14 / enriched.close) * 100 end as intraday_volatility,
    enriched.trend_regime,
    case
        when enriched.rsi_14 >= 60 then 'strengthening'
        when enriched.rsi_14 <= 40 then 'weakening'
        else 'neutral'
    end as momentum_regime,
    case
        when enriched.atr_14 > coalesce(enriched.atr_rolling_avg_20, enriched.atr_14) * 1.5 then 'extreme'
        when enriched.atr_14 > coalesce(enriched.atr_rolling_avg_20, enriched.atr_14) * 1.1 then 'elevated'
        else 'normal'
    end as volatility_regime,
    enriched.close >= enriched.rolling_high_20 as breakout_flag,
    enriched.close > enriched.ema_50 and enriched.close <= enriched.ema_20 as pullback_flag,
    case when enriched.close = 0 then null else ((enriched.rolling_high_20 - enriched.close) / enriched.close) * 100 end as distance_to_resistance_pct,
    case when enriched.close = 0 then null else ((enriched.close - enriched.rolling_low_20) / enriched.close) * 100 end as distance_to_support_pct,
    enriched.rsi_14,
    enriched.atr_14,
    enriched.volume_ma_20,
    enriched.sma_20,
    enriched.sma_50,
    enriched.ema_20,
    enriched.ema_50,
    (
        case when enriched.trend_regime = 'bullish' then 40 when enriched.trend_regime = 'bearish' then 25 else 10 end
        + case when enriched.rsi_14 between 55 and 70 then 20 when enriched.rsi_14 between 45 and 55 then 10 else 5 end
        + case when enriched.close >= enriched.rolling_high_20 then 20 else 0 end
        + case when enriched.close > enriched.ema_50 and enriched.close <= enriched.ema_20 then 20 else 0 end
    ) as signal_strength_score
from enriched
inner join latest_ticker
    on enriched.symbol = latest_ticker.symbol
where latest_ticker.reverse_row_num = 1
