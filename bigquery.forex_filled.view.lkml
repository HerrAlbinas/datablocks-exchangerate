view: bq_forex_filled {
  derived_table: {
    sql:
      with looker_table as (select * from `looker-datablocks.exchangerate.forex_real_full` order by exchange_date desc),
      days AS (
        SELECT day
        FROM (
          SELECT
            min(exchange_date) min_dt,
            CURRENT_DATE max_dt
          FROM looker_table
        ), UNNEST(GENERATE_DATE_ARRAY(min_dt, max_dt)) day
      )

      SELECT day as exchange_date, "EUR" as base_currency,
       LAST_VALUE(CNY IGNORE NULLS) OVER(ORDER BY day) CNY,
       LAST_VALUE(NOK IGNORE NULLS) OVER(ORDER BY day) NOK,
       LAST_VALUE(EUR IGNORE NULLS) OVER(ORDER BY day) EUR,
       LAST_VALUE(USD IGNORE NULLS) OVER(ORDER BY day) USD,
       LAST_VALUE(CAD IGNORE NULLS) OVER(ORDER BY day) CAD,
       LAST_VALUE(AUD IGNORE NULLS) OVER(ORDER BY day) AUD,
       LAST_VALUE(PLN IGNORE NULLS) OVER(ORDER BY day) PLN,
       LAST_VALUE(MXN IGNORE NULLS) OVER(ORDER BY day) MXN,
       LAST_VALUE(DKK IGNORE NULLS) OVER(ORDER BY day) DKK,
       LAST_VALUE(RUB IGNORE NULLS) OVER(ORDER BY day) RUB,
       LAST_VALUE(HUF IGNORE NULLS) OVER(ORDER BY day) HUF,
       LAST_VALUE(SEK IGNORE NULLS) OVER(ORDER BY day) SEK,
       LAST_VALUE(CZK IGNORE NULLS) OVER(ORDER BY day) CZK,
       LAST_VALUE(NZD IGNORE NULLS) OVER(ORDER BY day) NZD,
       LAST_VALUE(GBP IGNORE NULLS) OVER(ORDER BY day) GBP,
       LAST_VALUE(KRW IGNORE NULLS) OVER(ORDER BY day) KRW
      FROM days
      LEFT JOIN looker_table
      ON day = exchange_date
      order by exchange_date desc
       ;;
    datagroup_trigger: 24hours
  }

  dimension: cny {
    label: "CNY"
    type: number
    sql: ${TABLE}.CNY ;;
  }

  dimension: nok {
    label: "NOK"
    type: number
    sql: ${TABLE}.NOK ;;
  }

  dimension: eur {
    label: "EUR"
    type: number
    sql: ${TABLE}.EUR ;;
  }

  dimension: usd {
    label: "USD"
    type: number
    sql: ${TABLE}.USD ;;
  }

  dimension: cad {
    label: "CAD"
    type: number
    sql: ${TABLE}.CAD ;;
  }

  dimension: aud {
    label: "AUD"
    type: number
    sql: ${TABLE}.AUD ;;
  }

  dimension: pln {
    label: "PLN"
    type: number
    sql: ${TABLE}.PLN ;;
  }

  dimension: mxn {
    label: "MXN"
    type: number
    sql: ${TABLE}.MXN ;;
  }

  dimension: dkk {
    label: "DKK"
    type: number
    sql: ${TABLE}.DKK ;;
  }

  dimension: rub {
    label: "RUB"
    type: number
    sql: ${TABLE}.RUB ;;
  }

  dimension: huf {
    label: "HUF"
    type: number
    sql: ${TABLE}.HUF ;;
  }

  dimension: sek {
    label: "SEK"
    type: number
    sql: ${TABLE}.SEK ;;
  }

  dimension: czk {
    label: "CZK"
    type: number
    sql: ${TABLE}.CZK ;;
  }

  dimension: nzd {
    label: "NZD"
    type: number
    sql: ${TABLE}.NZD ;;
  }

  dimension: gbp {
    label: "GBP"
    type: number
    sql: ${TABLE}.GBP ;;
  }

  dimension: krw {
    label: "KRW"
    type: number
    sql: ${TABLE}.KRW ;;
  }

  dimension: base_currency {
    type: string
    sql: ${TABLE}.base_currency ;;
  }

  dimension_group: exchange {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.exchange_date ;;
  }

  }
