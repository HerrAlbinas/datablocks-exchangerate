view: bq_forex_historical_real {
  derived_table: {
    sql: with currency_table as
(select x.day
, (case when x.EUR_USD is null then
    (case when lag(x.EUR_USD, 1) over (order by x.day) is null then
      lag(x.EUR_USD, 2) over (order by x.day)
      Else lag(x.EUR_USD, 1) over (order by x.day) End)
    Else x.EUR_USD End) as EUR_USD
, (case when x.EUR_NZD is null then
    (case when lag(x.EUR_NZD, 1) over (order by x.day) is null then
      lag(x.EUR_NZD, 2) over (order by x.day)
      Else lag(x.EUR_NZD, 1) over (order by x.day) End)
    Else x.EUR_NZD End) as EUR_NZD
, (case when x.EUR_GBP is null then
    (case when lag(x.EUR_GBP, 1) over (order by x.day) is null then
      lag(x.EUR_GBP, 2) over (order by x.day)
      Else lag(x.EUR_GBP, 1) over (order by x.day) End)
    Else x.EUR_GBP End) as EUR_GBP
, (case when x.EUR_AUD is null then
    (case when lag(x.EUR_AUD, 1) over (order by x.day) is null then
      lag(x.EUR_AUD, 2) over (order by x.day)
      Else lag(x.EUR_AUD, 1) over (order by x.day) End)
    Else x.EUR_AUD End) as EUR_AUD
, (case when x.EUR_CAD is null then
    (case when lag(x.EUR_CAD, 1) over (order by x.day) is null then
      lag(x.EUR_CAD, 2) over (order by x.day)
      Else lag(x.EUR_CAD, 1) over (order by x.day) End)
    Else x.EUR_CAD End) as EUR_CAD
, (case when x.EUR_MXN is null then
    (case when lag(x.EUR_MXN, 1) over (order by x.day) is null then
      lag(x.EUR_MXN, 2) over (order by x.day)
      Else lag(x.EUR_MXN, 1) over (order by x.day) End)
    Else x.EUR_MXN End) as EUR_MXN,
(case when x.EUR_HUF is null then
    (case when lag(x.EUR_HUF, 1) over (order by x.day) is null then
      lag(x.EUR_HUF, 2) over (order by x.day)
      Else lag(x.EUR_HUF, 1) over (order by x.day) End)
    Else x.EUR_HUF End) as EUR_HUF,
(case when x.EUR_CZK is null then
    (case when lag(x.EUR_CZK, 1) over (order by x.day) is null then
      lag(x.EUR_CZK, 2) over (order by x.day)
      Else lag(x.EUR_CZK, 1) over (order by x.day) End)
    Else x.EUR_CZK End) as EUR_CZK,
(case when x.EUR_SEK is null then
    (case when lag(x.EUR_SEK, 1) over (order by x.day) is null then
      lag(x.EUR_SEK, 2) over (order by x.day)
      Else lag(x.EUR_SEK, 1) over (order by x.day) End)
    Else x.EUR_SEK End) as EUR_SEK,
(case when x.EUR_DKK is null then
    (case when lag(x.EUR_DKK, 1) over (order by x.day) is null then
      lag(x.EUR_DKK, 2) over (order by x.day)
      Else lag(x.EUR_DKK, 1) over (order by x.day) End)
    Else x.EUR_DKK End) as EUR_DKK,
(case when x.EUR_NOK is null then
    (case when lag(x.EUR_NOK, 1) over (order by x.day) is null then
      lag(x.EUR_NOK, 2) over (order by x.day)
      Else lag(x.EUR_NOK, 1) over (order by x.day) End)
    Else x.EUR_NOK End) as EUR_NOK,
(case when x.EUR_DKK is null then
    (case when lag(x.EUR_RUB, 1) over (order by x.day) is null then
      lag(x.EUR_RUB, 2) over (order by x.day)
      Else lag(x.EUR_RUB, 1) over (order by x.day) End)
    Else x.EUR_RUB End) as EUR_RUB,
(case when x.EUR_PLN is null then
    (case when lag(x.EUR_PLN, 1) over (order by x.day) is null then
      lag(x.EUR_PLN, 2) over (order by x.day)
      Else lag(x.EUR_PLN, 1) over (order by x.day) End)
    Else x.EUR_PLN End) as EUR_PLN,
(case when x.EUR_JPY is null then
    (case when lag(x.EUR_JPY, 1) over (order by x.day) is null then
      lag(x.EUR_JPY, 2) over (order by x.day)
      Else lag(x.EUR_JPY, 1) over (order by x.day) End)
    Else x.EUR_JPY End) as EUR_JPY,
(case when x.EUR_BYN is null then
    (case when lag(x.EUR_BYN, 1) over (order by x.day) is null then
      lag(x.EUR_BYN, 2) over (order by x.day)
      Else lag(x.EUR_BYN, 1) over (order by x.day) End)
    Else x.EUR_BYN End) as EUR_BYN,
(case when x.EUR_COP is null then
    (case when lag(x.EUR_COP, 1) over (order by x.day) is null then
      lag(x.EUR_COP, 2) over (order by x.day)
      Else lag(x.EUR_COP, 1) over (order by x.day) End)
    Else x.EUR_COP End) as EUR_COP,
(case when x.EUR_KRW is null then
    (case when lag(x.EUR_KRW, 1) over (order by x.day) is null then
      lag(x.EUR_KRW, 2) over (order by x.day)
      Else lag(x.EUR_KRW, 1) over (order by x.day) End)
    Else x.EUR_KRW End) as EUR_KRW,
(case when x.EUR_ARS is null then
    (case when lag(x.EUR_ARS, 1) over (order by x.day) is null then
      lag(x.EUR_ARS, 2) over (order by x.day)
      Else lag(x.EUR_ARS, 1) over (order by x.day) End)
    Else x.EUR_ARS End) as EUR_ARS,
(case when x.EUR_BRL is null then
    (case when lag(x.EUR_BRL, 1) over (order by x.day) is null then
      lag(x.EUR_BRL, 2) over (order by x.day)
      Else lag(x.EUR_BRL, 1) over (order by x.day) End)
    Else x.EUR_BRL End) as EUR_BRL,
(case when x.EUR_CNY is null then
    (case when lag(x.EUR_CNY, 1) over (order by x.day) is null then
      lag(x.EUR_CNY, 2) over (order by x.day)
      Else lag(x.EUR_CNY, 1) over (order by x.day) End)
    Else x.EUR_CNY End) as EUR_CNY,
(case when x.EUR_INR is null then
    (case when lag(x.EUR_INR, 1) over (order by x.day) is null then
      lag(x.EUR_INR, 2) over (order by x.day)
      Else lag(x.EUR_INR, 1) over (order by x.day) End)
    Else x.EUR_INR End) as EUR_INR,
(case when x.EUR_THB is null then
    (case when lag(x.EUR_THB, 1) over (order by x.day) is null then
      lag(x.EUR_THB, 2) over (order by x.day)
      Else lag(x.EUR_THB, 1) over (order by x.day) End)
    Else x.EUR_THB End) as EUR_THB,
(case when x.EUR_PHP is null then
    (case when lag(x.EUR_PHP, 1) over (order by x.day) is null then
      lag(x.EUR_PHP, 2) over (order by x.day)
      Else lag(x.EUR_PHP, 1) over (order by x.day) End)
    Else x.EUR_PHP End) as EUR_PHP,
(case when x.EUR_RSD is null then
    (case when lag(x.EUR_RSD, 1) over (order by x.day) is null then
      lag(x.EUR_RSD, 2) over (order by x.day)
      Else lag(x.EUR_RSD, 1) over (order by x.day) End)
    Else x.EUR_RSD End) as EUR_RSD,
(case when x.EUR_NGN is null then
    (case when lag(x.EUR_NGN, 1) over (order by x.day) is null then
      lag(x.EUR_NGN, 2) over (order by x.day)
      Else lag(x.EUR_NGN, 1) over (order by x.day) End)
    Else x.EUR_NGN End) as EUR_NGN,
(case when x.EUR_CRC is null then
    (case when lag(x.EUR_CRC, 1) over (order by x.day) is null then
      lag(x.EUR_CRC, 2) over (order by x.day)
      Else lag(x.EUR_CRC, 1) over (order by x.day) End)
    Else x.EUR_CRC End) as EUR_CRC,
(case when x.EUR_TWD is null then
    (case when lag(x.EUR_TWD, 1) over (order by x.day) is null then
      lag(x.EUR_TWD, 2) over (order by x.day)
      Else lag(x.EUR_TWD, 1) over (order by x.day) End)
    Else x.EUR_TWD End) as EUR_TWD,
(case when x.EUR_CHF is null then
    (case when lag(x.EUR_CHF, 1) over (order by x.day) is null then
      lag(x.EUR_CHF, 2) over (order by x.day)
      Else lag(x.EUR_CHF, 1) over (order by x.day) End)
    Else x.EUR_CHF End) as EUR_CHF,
(case when x.EUR_QAR is null then
    (case when lag(x.EUR_QAR, 1) over (order by x.day) is null then
      lag(x.EUR_QAR, 2) over (order by x.day)
      Else lag(x.EUR_QAR, 1) over (order by x.day) End)
    Else x.EUR_QAR End) as EUR_QAR,
(case when x.EUR_CLP is null then
    (case when lag(x.EUR_CLP, 1) over (order by x.day) is null then
      lag(x.EUR_CLP, 2) over (order by x.day)
      Else lag(x.EUR_CLP, 1) over (order by x.day) End)
    Else x.EUR_CLP End) as EUR_CLP,
(case when x.EUR_LBP is null then
    (case when lag(x.EUR_LBP, 1) over (order by x.day) is null then
      lag(x.EUR_LBP, 2) over (order by x.day)
      Else lag(x.EUR_LBP, 1) over (order by x.day) End)
    Else x.EUR_LBP End) as EUR_LBP,
(case when x.EUR_ZAR is null then
    (case when lag(x.EUR_ZAR, 1) over (order by x.day) is null then
      lag(x.EUR_ZAR, 2) over (order by x.day)
      Else lag(x.EUR_ZAR, 1) over (order by x.day) End)
    Else x.EUR_ZAR End) as EUR_ZAR,
(case when x.EUR_SGD is null then
    (case when lag(x.EUR_SGD, 1) over (order by x.day) is null then
      lag(x.EUR_SGD, 2) over (order by x.day)
      Else lag(x.EUR_SGD, 1) over (order by x.day) End)
    Else x.EUR_SGD End) as EUR_SGD,
(case when x.EUR_SAR is null then
    (case when lag(x.EUR_SAR, 1) over (order by x.day) is null then
      lag(x.EUR_SAR, 2) over (order by x.day)
      Else lag(x.EUR_SAR, 1) over (order by x.day) End)
    Else x.EUR_SAR End) as EUR_SAR,
(case when x.EUR_AED is null then
    (case when lag(x.EUR_AED, 1) over (order by x.day) is null then
      lag(x.EUR_AED, 2) over (order by x.day)
      Else lag(x.EUR_AED, 1) over (order by x.day) End)
    Else x.EUR_AED End) as EUR_AED,
(case when x.EUR_TRY is null then
    (case when lag(x.EUR_TRY, 1) over (order by x.day) is null then
      lag(x.EUR_TRY, 2) over (order by x.day)
      Else lag(x.EUR_TRY, 1) over (order by x.day) End)
    Else x.EUR_TRY End) as EUR_TRY,
(case when x.EUR_RON is null then
    (case when lag(x.EUR_RON, 1) over (order by x.day) is null then
      lag(x.EUR_RON, 2) over (order by x.day)
      Else lag(x.EUR_RON, 1) over (order by x.day) End)
    Else x.EUR_RON End) as EUR_RON,
(case when x.EUR_HKD is null then
    (case when lag(x.EUR_HKD, 1) over (order by x.day) is null then
      lag(x.EUR_HKD, 2) over (order by x.day)
      Else lag(x.EUR_HKD, 1) over (order by x.day) End)
    Else x.EUR_HKD End) as EUR_HKD,
(case when x.EUR_IDR is null then
    (case when lag(x.EUR_IDR, 1) over (order by x.day) is null then
      lag(x.EUR_IDR, 2) over (order by x.day)
      Else lag(x.EUR_IDR, 1) over (order by x.day) End)
    Else x.EUR_IDR End) as EUR_IDR


from
(WITH
      splitted AS (SELECT *
        FROM
          UNNEST( SPLIT(RPAD('',
                1 + DATE_DIFF(CURRENT_DATE(), DATE("1999-01-01"), DAY),
                '.'),''))),
        with_row_numbers AS (
        SELECT
          ROW_NUMBER() OVER() AS pos, *
        FROM splitted), calendar_day AS (
        SELECT
          cast(DATE_ADD(DATE("1999-01-01"), INTERVAL (pos - 1) DAY) as timestamp)AS day
        FROM with_row_numbers)
      SELECT *
      FROM calendar_day
      left join (
      SELECT
        cast(forex_real.exchange_date as timestamp) AS forex_exchange_date,
        1 as EUR,
        forex_real.USD  AS EUR_USD,
        forex_real.NZD AS EUR_NZD,
        forex_real.GBP AS EUR_GBP,
        forex_real.AUD AS EUR_AUD,
        forex_real.MXN AS EUR_MXN,
        forex_real.CAD AS EUR_CAD,
        forex_real.HUF AS EUR_HUF,
        forex_real.CZK AS EUR_CZK,
        forex_real.SEK as EUR_SEK,
        forex_real.DKK as EUR_DKK,
        forex_real.NOK as EUR_NOK,
        forex_real.RUB as EUR_RUB,
        forex_real.PLN as EUR_PLN,
        forex_real.JPY as EUR_JPY,
        forex_real.BYN as EUR_BYN,
        forex_real.COP as EUR_COP,
        forex_real.KRW as EUR_KRW,
        forex_real.ARS as EUR_ARS,
        forex_real.BRL as EUR_BRL,
        forex_real.CNY as EUR_CNY,
        forex_real.INR AS EUR_INR,
        forex_real.THB AS EUR_THB,
        forex_real.PHP AS EUR_PHP,
        forex_real.RSD AS EUR_RSD,
        forex_real.RSD AS EUR_NGN,
        forex_real.RSD AS EUR_CRC,
        forex_real.RSD AS EUR_TWD,
        forex_real.RSD AS EUR_CHF,
        forex_real.RSD AS EUR_HKD,
        forex_real.RSD AS EUR_QAR,
        forex_real.RSD AS EUR_CLP,
        forex_real.RSD AS EUR_LBP,
        forex_real.RSD AS EUR_ZAR,
        forex_real.RSD AS EUR_SGD,
        forex_real.RSD AS EUR_SAR,
        forex_real.RSD AS EUR_AED,
        forex_real.RSD AS EUR_TRY,
        forex_real.RSD AS EUR_RON,
        forex_real.RSD AS EUR_MYR,
        forex_real.RSD AS EUR_EGP,
        forex_real.RSD AS EUR_ILS,
        forex_real.RSD AS EUR_PEN,
        forex_real.RSD AS EUR_NGN,
        forex_real.IDR as EUR_IDR

      FROM `looker-datablocks.exchangerate.forex_real_full` AS forex_real
      Group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) as forex
    on forex.forex_exchange_date = calendar_day.day) as x
    order by day desc)

    select day, EUR_USD as rate, "USD" as currency from currency_table
UNION ALL
  select day, EUR_NZD as rate, "NZD" as currency from currency_table
UNION ALL
  select day, EUR_GBP as rate, "GBP" as currency from currency_table
UNION ALL
  select day, EUR_AUD as rate, "AUD" as currency from currency_table
UNION ALL
  select day, EUR_MXN as rate, "MXN" as currency from currency_table
UNION ALL
  select day, EUR_CAD as rate, "CAD" as currency from currency_table
UNION ALL
  select day, EUR_HUF as rate, "HUF" as currency from currency_table
UNION ALL
  select day, EUR_CZK as rate, "CZK" as currency from currency_table
UNION ALL
  select day, EUR_SEK as rate, "SEK" as currency from currency_table
UNION ALL
  select day, EUR_DKK as rate, "DKK" as currency from currency_table
UNION ALL
  select day, EUR_NOK as rate, "NOK" as currency from currency_table
UNION ALL
  select day, EUR_RUB as rate, "RUB" as currency from currency_table
UNION ALL
  select day, EUR_PLN as rate, "PLN" as currency from currency_table
UNION ALL
  select day, EUR_JPY as rate, "JPY" as currency from currency_table
UNION ALL
  select day, EUR_BYN as rate, "BYN" as currency from currency_table
UNION ALL
  select day, EUR_COP as rate, "COP" as currency from currency_table
UNION ALL
  select day, EUR_KRW as rate, "KRW" as currency from currency_table
UNION ALL
  select day, EUR_ARS as rate, "ARS" as currency from currency_table
UNION ALL
  select day, EUR_BRL as rate, "BRL" as currency from currency_table
UNION ALL
  select day, EUR_CNY as rate, "CNY" as currency from currency_table
UNION ALL
  select day, EUR_INR as rate, "INR" as currency from currency_table
UNION ALL
  select day, EUR_THB as rate, "THB" as currency from currency_table
UNION ALL
  select day, EUR_PHP as rate, "PHP" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "RSD" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "NGN" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "CRC" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "TWD" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "CHF" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "HKD" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "QAR" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "CLP" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "LBP" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "ZAR" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "SGD" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "SAR" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "AED" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "TRY" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "RON" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "MYR" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "EGP" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "ILS" as currency from currency_table
UNION ALL
  select day, EUR_RSD as rate, "PEN" as currency from currency_table
UNION ALL
  select day, EUR_IDR as rate, "IDR" as currency from currency_table
UNION ALL
  select day, 1 as rate, "EUR" as currency from currency_table

  order by day desc ;;

  datagroup_trigger: 24hours
  }

  dimension_group: forex_exchange {
    label: "Date"
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.day ;;
  }

  dimension: rate {
    label: "Exchange Rate"
    description: "1 Euro = X units"
    value_format_name: decimal_4
    type:  number
    sql: ${TABLE}.rate ;;
    # hidden: yes
  }

  dimension: currency {
    label: "Currency Code"
    type:  string
    sql: ${TABLE}.currency ;;
  }


  ################################### measures to plot on graph ###################################

  measure: exchrate {
    label: "ExchangeRate"
    description: "1 Euro = X units"
    value_format_name: decimal_4
    type:  max
    sql: ${TABLE}.rate ;;
  }

}
