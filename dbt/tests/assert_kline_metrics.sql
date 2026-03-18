select *
from {{ ref('mart_btc_ohlcv') }}
where open <= 0
   or high <= 0
   or low <= 0
   or close <= 0
   or high < low
   or volume < 0
   or quote_asset_volume < 0
   or (rsi_14 is not null and (rsi_14 < 0 or rsi_14 > 100))
   or (atr_14 is not null and atr_14 < 0)
