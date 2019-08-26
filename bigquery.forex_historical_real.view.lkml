view: bq_forex_historical_real {
  derived_table: {
    sql: select x.day
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
    order by day desc
       ;;
    datagroup_trigger: default
  }

  dimension_group: forex_exchange {
    type: time
    timeframes: [date, week, month, year]
    sql: ${TABLE}.day ;;
  }


  dimension: eur_usd {
    label: "EUR/USD"
    description: "1 Euro = X US Dollars"
    value_format_name: decimal_4
    type:  number
    sql: ${TABLE}.EUR_USD ;;
    hidden: yes
  }

  dimension: eur_nzd {
    label: "EUR/NZD"
    description: "1 Euro = X New Zealand Dollars"
    value_format_name: decimal_4
    type:  number
    sql: ${TABLE}.EUR_NZD ;;
    hidden: yes
  }

  dimension: eur_aud {
    label: "EUR/AUD"
    description: "1 Euro = X Australian Dollars"
    value_format_name: decimal_4
    type:  number
    sql: ${TABLE}.EUR_AUD ;;
    hidden: yes
  }

  dimension: eur_mxn {
    label: "EUR/MXN"
    description: "1 Euro = X Mexican Pesos"
    value_format_name: decimal_4
    type:  number
    sql: ${TABLE}.EUR_MXN ;;
    hidden: yes
  }

  dimension: eur_cad {
    label: "EUR/XAD"
    description: "1 Euro = X Canadian Dollars"
    value_format_name: decimal_4
    type:  number
    sql: ${TABLE}.EUR_JPY ;;
    hidden: yes
  }

  dimension: eur_gbp {
    label: "EUR/GBP"
    description: "1 Euro = X Great Britain Pounds"
    type:  number
    sql: ${TABLE}.EUR_GBP ;;
    hidden: yes
  }

  ################################### measures to plot on graph ###################################

  measure: eurusd {
    label: "EUR/USD"
    description: "1 Euro = X US Dollars"
    value_format_name: decimal_4
    type:  max
    sql: ${TABLE}.EUR_USD ;;
  }

  measure: eurnzd {
    label: "EUR/NZD"
    description: "1 Euro = X New Zealand Dollars"
    value_format_name: decimal_4
    type:  max
    sql: ${TABLE}.EUR_NZD ;;
  }

  measure: euraud {
    label: "EUR/AUD"
    description: "1 Euro = X Australian Dollars"
    value_format_name: decimal_4
    type:  max
    sql: ${TABLE}.EUR_AUD ;;
  }

  measure: eurmxn {
    label: "EUR/MXN"
    description: "1 Euro = X Mexican Pesos"
    value_format_name: decimal_4
    type:  max
    sql: ${TABLE}.EUR_MXN ;;
  }

  measure: eurcad {
    label: "EUR/XAD"
    description: "1 Euro = X Canadian Dollars"
    value_format_name: decimal_4
    type:  max
    sql: ${TABLE}.EUR_JPY ;;
  }

  measure: eurgbp {
    label: "EUR/GBP"
    description: "1 Euro = X Great Britain Pounds"
    type:  max
    sql: ${TABLE}.EUR_GBP ;;
  }
}
