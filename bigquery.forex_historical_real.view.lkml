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
    Else x.EUR_MXN End) as EUR_MXN

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
        forex_real.USD  AS EUR_USD,
        forex_real.NZD AS EUR_NZD,
        forex_real.GBP AS EUR_GBP,
        forex_real.AUD AS EUR_AUD,
        forex_real.MXN AS EUR_MXN,
        forex_real.CAD AS EUR_CAD
      FROM `looker-datablocks.exchangerate.forex_real_full`  AS forex_real
      Group by 1,2,3,4,5,6,7
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
