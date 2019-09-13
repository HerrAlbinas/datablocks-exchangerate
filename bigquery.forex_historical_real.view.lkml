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
    Else x.EUR_BYN End) as EUR_BYN

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
        forex_real.JPY as EUR_BYN

      FROM `looker-datablocks.exchangerate.forex_real_full`  AS forex_real
      Group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16, 17
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


  order by day desc ;;
    datagroup_trigger: 24hours
  }

  dimension_group: forex_exchange {
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.day ;;
  }

  dimension: rate {
    label: "ExchangeRate"
    description: "1 Euro = X units"
    value_format_name: decimal_4
    type:  number
    sql: ${TABLE}.rate ;;
    hidden: yes
  }

  dimension: currency {
    label: "CurrencyCode"
    description: "Currency Code"
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
