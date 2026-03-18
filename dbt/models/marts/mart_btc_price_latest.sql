with candles as (
    select
        *,
        lag(close, 1) over (partition by symbol, interval order by candle_time) as close_5m_ago,
        lag(close, 3) over (partition by symbol, interval order by candle_time) as close_15m_ago,
        lag(close, 12) over (partition by symbol, interval order by candle_time) as close_1h_ago,
        lag(close, 48) over (partition by symbol, interval order by candle_time) as close_4h_ago,
        lag(close, 288) over (partition by symbol, interval order by candle_time) as close_24h_ago,
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
latest_ticker as (
    select
        *,
        row_number() over (partition by symbol order by event_time desc) as reverse_row_num
    from {{ ref('stg_btc_ticker') }}
)
select
    candles.symbol,
    candles.interval,
    candles.candle_time as as_of_time,
    candles.close as last_price,
    latest_ticker.price_change_pct_24h as price_change_pct_24h_exchange,
    case when candles.close_5m_ago is null or candles.close_5m_ago = 0 then null else ((candles.close - candles.close_5m_ago) / candles.close_5m_ago) * 100 end as price_change_pct_5m,
    case when candles.close_15m_ago is null or candles.close_15m_ago = 0 then null else ((candles.close - candles.close_15m_ago) / candles.close_15m_ago) * 100 end as price_change_pct_15m,
    case when candles.close_1h_ago is null or candles.close_1h_ago = 0 then null else ((candles.close - candles.close_1h_ago) / candles.close_1h_ago) * 100 end as price_change_pct_1h,
    case when candles.close_4h_ago is null or candles.close_4h_ago = 0 then null else ((candles.close - candles.close_4h_ago) / candles.close_4h_ago) * 100 end as price_change_pct_4h,
    case when candles.close_24h_ago is null or candles.close_24h_ago = 0 then null else ((candles.close - candles.close_24h_ago) / candles.close_24h_ago) * 100 end as price_change_pct_24h,
    latest_ticker.volume_24h,
    latest_ticker.quote_volume_24h,
    case when candles.close = 0 then null else (candles.atr_14 / candles.close) * 100 end as intraday_volatility,
    candles.trend_regime,
    case
        when candles.rsi_14 >= 60 then 'strengthening'
        when candles.rsi_14 <= 40 then 'weakening'
        else 'neutral'
    end as momentum_regime,
    case
        when candles.atr_14 > coalesce(candles.atr_rolling_avg_20, candles.atr_14) * 1.5 then 'extreme'
        when candles.atr_14 > coalesce(candles.atr_rolling_avg_20, candles.atr_14) * 1.1 then 'elevated'
        else 'normal'
    end as volatility_regime,
    candles.close >= candles.rolling_high_20 as breakout_flag,
    candles.close > candles.ema_50 and candles.close <= candles.ema_20 as pullback_flag,
    case when candles.close = 0 then null else ((candles.rolling_high_20 - candles.close) / candles.close) * 100 end as distance_to_resistance_pct,
    case when candles.close = 0 then null else ((candles.close - candles.rolling_low_20) / candles.close) * 100 end as distance_to_support_pct,
    candles.rsi_14,
    candles.atr_14,
    candles.volume_ma_20,
    candles.sma_20,
    candles.sma_50,
    candles.ema_20,
    candles.ema_50,
    (
        case when candles.trend_regime = 'bullish' then 40 when candles.trend_regime = 'bearish' then 25 else 10 end
        + case when candles.rsi_14 between 55 and 70 then 20 when candles.rsi_14 between 45 and 55 then 10 else 5 end
        + case when candles.close >= candles.rolling_high_20 then 20 else 0 end
        + case when candles.close > candles.ema_50 and candles.close <= candles.ema_20 then 20 else 0 end
    ) as signal_strength_score
from candles
inner join latest_ticker
    on candles.symbol = latest_ticker.symbol
where candles.reverse_row_num = 1
  and latest_ticker.reverse_row_num = 1
