view: bq_forex_historical_real {
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
      ),

      bq_filled as (
      SELECT day as exchange_date, "EUR" as base_currency
    , LAST_VALUE(AFN IGNORE NULLS) OVER(ORDER BY day) AFN
    , LAST_VALUE(AMD IGNORE NULLS) OVER(ORDER BY day) AMD
    , LAST_VALUE(ANG IGNORE NULLS) OVER(ORDER BY day) ANG
    , LAST_VALUE(AOA IGNORE NULLS) OVER(ORDER BY day) AOA
    , LAST_VALUE(ARS IGNORE NULLS) OVER(ORDER BY day) ARS
    , LAST_VALUE(AUD IGNORE NULLS) OVER(ORDER BY day) AUD
    , LAST_VALUE(AWG IGNORE NULLS) OVER(ORDER BY day) AWG
    , LAST_VALUE(AZN IGNORE NULLS) OVER(ORDER BY day) AZN
    , LAST_VALUE(BAM IGNORE NULLS) OVER(ORDER BY day) BAM
    , LAST_VALUE(BBD IGNORE NULLS) OVER(ORDER BY day) BBD
    , LAST_VALUE(BDT IGNORE NULLS) OVER(ORDER BY day) BDT
    , LAST_VALUE(BGN IGNORE NULLS) OVER(ORDER BY day) BGN
    , LAST_VALUE(BHD IGNORE NULLS) OVER(ORDER BY day) BHD
    , LAST_VALUE(BIF IGNORE NULLS) OVER(ORDER BY day) BIF
    , LAST_VALUE(BMD IGNORE NULLS) OVER(ORDER BY day) BMD
    , LAST_VALUE(BND IGNORE NULLS) OVER(ORDER BY day) BND
    , LAST_VALUE(BOB IGNORE NULLS) OVER(ORDER BY day) BOB
    , LAST_VALUE(BRL IGNORE NULLS) OVER(ORDER BY day) BRL
    , LAST_VALUE(BSD IGNORE NULLS) OVER(ORDER BY day) BSD
    , LAST_VALUE(BTC IGNORE NULLS) OVER(ORDER BY day) BTC
    , LAST_VALUE(BTN IGNORE NULLS) OVER(ORDER BY day) BTN
    , LAST_VALUE(BWP IGNORE NULLS) OVER(ORDER BY day) BWP
    , LAST_VALUE(BYN IGNORE NULLS) OVER(ORDER BY day) BYN
    , LAST_VALUE(BYR IGNORE NULLS) OVER(ORDER BY day) BYR
    , LAST_VALUE(BZD IGNORE NULLS) OVER(ORDER BY day) BZD
    , LAST_VALUE(CAD IGNORE NULLS) OVER(ORDER BY day) CAD
    , LAST_VALUE(CDF IGNORE NULLS) OVER(ORDER BY day) CDF
    , LAST_VALUE(CHF IGNORE NULLS) OVER(ORDER BY day) CHF
    , LAST_VALUE(CLF IGNORE NULLS) OVER(ORDER BY day) CLF
    , LAST_VALUE(CLP IGNORE NULLS) OVER(ORDER BY day) CLP
    , LAST_VALUE(CNY IGNORE NULLS) OVER(ORDER BY day) CNY
    , LAST_VALUE(COP IGNORE NULLS) OVER(ORDER BY day) COP
    , LAST_VALUE(CRC IGNORE NULLS) OVER(ORDER BY day) CRC
    , LAST_VALUE(CUC IGNORE NULLS) OVER(ORDER BY day) CUC
    , LAST_VALUE(CUP IGNORE NULLS) OVER(ORDER BY day) CUP
    , LAST_VALUE(CVE IGNORE NULLS) OVER(ORDER BY day) CVE
    , LAST_VALUE(CZK IGNORE NULLS) OVER(ORDER BY day) CZK
    , LAST_VALUE(DJF IGNORE NULLS) OVER(ORDER BY day) DJF
    , LAST_VALUE(DKK IGNORE NULLS) OVER(ORDER BY day) DKK
    , LAST_VALUE(DOP IGNORE NULLS) OVER(ORDER BY day) DOP
    , LAST_VALUE(DZD IGNORE NULLS) OVER(ORDER BY day) DZD
    , LAST_VALUE(EGP IGNORE NULLS) OVER(ORDER BY day) EGP
    , LAST_VALUE(ERN IGNORE NULLS) OVER(ORDER BY day) ERN
    , LAST_VALUE(ETB IGNORE NULLS) OVER(ORDER BY day) ETB
    , LAST_VALUE(EUR IGNORE NULLS) OVER(ORDER BY day) EUR
    , LAST_VALUE(FJD IGNORE NULLS) OVER(ORDER BY day) FJD
    , LAST_VALUE(FKP IGNORE NULLS) OVER(ORDER BY day) FKP
    , LAST_VALUE(GBP IGNORE NULLS) OVER(ORDER BY day) GBP
    , LAST_VALUE(GEL IGNORE NULLS) OVER(ORDER BY day) GEL
    , LAST_VALUE(GGP IGNORE NULLS) OVER(ORDER BY day) GGP
    , LAST_VALUE(GHS IGNORE NULLS) OVER(ORDER BY day) GHS
    , LAST_VALUE(GIP IGNORE NULLS) OVER(ORDER BY day) GIP
    , LAST_VALUE(GMD IGNORE NULLS) OVER(ORDER BY day) GMD
    , LAST_VALUE(GNF IGNORE NULLS) OVER(ORDER BY day) GNF
    , LAST_VALUE(GTQ IGNORE NULLS) OVER(ORDER BY day) GTQ
    , LAST_VALUE(GYD IGNORE NULLS) OVER(ORDER BY day) GYD
    , LAST_VALUE(HKD IGNORE NULLS) OVER(ORDER BY day) HKD
    , LAST_VALUE(HNL IGNORE NULLS) OVER(ORDER BY day) HNL
    , LAST_VALUE(HRK IGNORE NULLS) OVER(ORDER BY day) HRK
    , LAST_VALUE(HTG IGNORE NULLS) OVER(ORDER BY day) HTG
    , LAST_VALUE(HUF IGNORE NULLS) OVER(ORDER BY day) HUF
    , LAST_VALUE(IDR IGNORE NULLS) OVER(ORDER BY day) IDR
    , LAST_VALUE(ILS IGNORE NULLS) OVER(ORDER BY day) ILS
    , LAST_VALUE(IMP IGNORE NULLS) OVER(ORDER BY day) IMP
    , LAST_VALUE(INR IGNORE NULLS) OVER(ORDER BY day) INR
    , LAST_VALUE(IQD IGNORE NULLS) OVER(ORDER BY day) IQD
    , LAST_VALUE(IRR IGNORE NULLS) OVER(ORDER BY day) IRR
    , LAST_VALUE(ISK IGNORE NULLS) OVER(ORDER BY day) ISK
    , LAST_VALUE(JEP IGNORE NULLS) OVER(ORDER BY day) JEP
    , LAST_VALUE(JMD IGNORE NULLS) OVER(ORDER BY day) JMD
    , LAST_VALUE(JOD IGNORE NULLS) OVER(ORDER BY day) JOD
    , LAST_VALUE(JPY IGNORE NULLS) OVER(ORDER BY day) JPY
    , LAST_VALUE(KES IGNORE NULLS) OVER(ORDER BY day) KES
    , LAST_VALUE(KGS IGNORE NULLS) OVER(ORDER BY day) KGS
    , LAST_VALUE(KHR IGNORE NULLS) OVER(ORDER BY day) KHR
    , LAST_VALUE(KMF IGNORE NULLS) OVER(ORDER BY day) KMF
    , LAST_VALUE(KPW IGNORE NULLS) OVER(ORDER BY day) KPW
    , LAST_VALUE(KRW IGNORE NULLS) OVER(ORDER BY day) KRW
    , LAST_VALUE(KWD IGNORE NULLS) OVER(ORDER BY day) KWD
    , LAST_VALUE(KYD IGNORE NULLS) OVER(ORDER BY day) KYD
    , LAST_VALUE(KZT IGNORE NULLS) OVER(ORDER BY day) KZT
    , LAST_VALUE(LAK IGNORE NULLS) OVER(ORDER BY day) LAK
    , LAST_VALUE(LBP IGNORE NULLS) OVER(ORDER BY day) LBP
    , LAST_VALUE(LKR IGNORE NULLS) OVER(ORDER BY day) LKR
    , LAST_VALUE(LRD IGNORE NULLS) OVER(ORDER BY day) LRD
    , LAST_VALUE(LSL IGNORE NULLS) OVER(ORDER BY day) LSL
    , LAST_VALUE(LTL IGNORE NULLS) OVER(ORDER BY day) LTL
    , LAST_VALUE(LVL IGNORE NULLS) OVER(ORDER BY day) LVL
    , LAST_VALUE(LYD IGNORE NULLS) OVER(ORDER BY day) LYD
    , LAST_VALUE(MAD IGNORE NULLS) OVER(ORDER BY day) MAD
    , LAST_VALUE(MDL IGNORE NULLS) OVER(ORDER BY day) MDL
    , LAST_VALUE(MGA IGNORE NULLS) OVER(ORDER BY day) MGA
    , LAST_VALUE(MKD IGNORE NULLS) OVER(ORDER BY day) MKD
    , LAST_VALUE(MMK IGNORE NULLS) OVER(ORDER BY day) MMK
    , LAST_VALUE(MNT IGNORE NULLS) OVER(ORDER BY day) MNT
    , LAST_VALUE(MOP IGNORE NULLS) OVER(ORDER BY day) MOP
    , LAST_VALUE(MRO IGNORE NULLS) OVER(ORDER BY day) MRO
    , LAST_VALUE(MUR IGNORE NULLS) OVER(ORDER BY day) MUR
    , LAST_VALUE(MVR IGNORE NULLS) OVER(ORDER BY day) MVR
    , LAST_VALUE(MWK IGNORE NULLS) OVER(ORDER BY day) MWK
    , LAST_VALUE(MXN IGNORE NULLS) OVER(ORDER BY day) MXN
    , LAST_VALUE(MYR IGNORE NULLS) OVER(ORDER BY day) MYR
    , LAST_VALUE(MZN IGNORE NULLS) OVER(ORDER BY day) MZN
    , LAST_VALUE(NAD IGNORE NULLS) OVER(ORDER BY day) NAD
    , LAST_VALUE(NGN IGNORE NULLS) OVER(ORDER BY day) NGN
    , LAST_VALUE(NIO IGNORE NULLS) OVER(ORDER BY day) NIO
    , LAST_VALUE(NOK IGNORE NULLS) OVER(ORDER BY day) NOK
    , LAST_VALUE(NPR IGNORE NULLS) OVER(ORDER BY day) NPR
    , LAST_VALUE(NZD IGNORE NULLS) OVER(ORDER BY day) NZD
    , LAST_VALUE(OMR IGNORE NULLS) OVER(ORDER BY day) OMR
    , LAST_VALUE(PAB IGNORE NULLS) OVER(ORDER BY day) PAB
    , LAST_VALUE(PEN IGNORE NULLS) OVER(ORDER BY day) PEN
    , LAST_VALUE(PGK IGNORE NULLS) OVER(ORDER BY day) PGK
    , LAST_VALUE(PHP IGNORE NULLS) OVER(ORDER BY day) PHP
    , LAST_VALUE(PKR IGNORE NULLS) OVER(ORDER BY day) PKR
    , LAST_VALUE(PLN IGNORE NULLS) OVER(ORDER BY day) PLN
    , LAST_VALUE(PYG IGNORE NULLS) OVER(ORDER BY day) PYG
    , LAST_VALUE(QAR IGNORE NULLS) OVER(ORDER BY day) QAR
    , LAST_VALUE(RON IGNORE NULLS) OVER(ORDER BY day) RON
    , LAST_VALUE(RSD IGNORE NULLS) OVER(ORDER BY day) RSD
    , LAST_VALUE(RUB IGNORE NULLS) OVER(ORDER BY day) RUB
    , LAST_VALUE(RWF IGNORE NULLS) OVER(ORDER BY day) RWF
    , LAST_VALUE(SAR IGNORE NULLS) OVER(ORDER BY day) SAR
    , LAST_VALUE(SBD IGNORE NULLS) OVER(ORDER BY day) SBD
    , LAST_VALUE(SCR IGNORE NULLS) OVER(ORDER BY day) SCR
    , LAST_VALUE(SDG IGNORE NULLS) OVER(ORDER BY day) SDG
    , LAST_VALUE(SEK IGNORE NULLS) OVER(ORDER BY day) SEK
    , LAST_VALUE(SGD IGNORE NULLS) OVER(ORDER BY day) SGD
    , LAST_VALUE(SHP IGNORE NULLS) OVER(ORDER BY day) SHP
    , LAST_VALUE(SLL IGNORE NULLS) OVER(ORDER BY day) SLL
    , LAST_VALUE(SOS IGNORE NULLS) OVER(ORDER BY day) SOS
    , LAST_VALUE(SRD IGNORE NULLS) OVER(ORDER BY day) SRD
    , LAST_VALUE(STD IGNORE NULLS) OVER(ORDER BY day) STD
    , LAST_VALUE(SVC IGNORE NULLS) OVER(ORDER BY day) SVC
    , LAST_VALUE(SYP IGNORE NULLS) OVER(ORDER BY day) SYP
    , LAST_VALUE(SZL IGNORE NULLS) OVER(ORDER BY day) SZL
    , LAST_VALUE(THB IGNORE NULLS) OVER(ORDER BY day) THB
    , LAST_VALUE(TJS IGNORE NULLS) OVER(ORDER BY day) TJS
    , LAST_VALUE(TMT IGNORE NULLS) OVER(ORDER BY day) TMT
    , LAST_VALUE(TND IGNORE NULLS) OVER(ORDER BY day) TND
    , LAST_VALUE(TOP IGNORE NULLS) OVER(ORDER BY day) TOP
    , LAST_VALUE(TRY IGNORE NULLS) OVER(ORDER BY day) TRY
    , LAST_VALUE(TTD IGNORE NULLS) OVER(ORDER BY day) TTD
    , LAST_VALUE(TWD IGNORE NULLS) OVER(ORDER BY day) TWD
    , LAST_VALUE(TZS IGNORE NULLS) OVER(ORDER BY day) TZS
    , LAST_VALUE(UAH IGNORE NULLS) OVER(ORDER BY day) UAH
    , LAST_VALUE(UGX IGNORE NULLS) OVER(ORDER BY day) UGX
    , LAST_VALUE(USD IGNORE NULLS) OVER(ORDER BY day) USD
    , LAST_VALUE(UYU IGNORE NULLS) OVER(ORDER BY day) UYU
    , LAST_VALUE(UZS IGNORE NULLS) OVER(ORDER BY day) UZS
    , LAST_VALUE(VEF IGNORE NULLS) OVER(ORDER BY day) VEF
    , LAST_VALUE(VND IGNORE NULLS) OVER(ORDER BY day) VND
    , LAST_VALUE(VUV IGNORE NULLS) OVER(ORDER BY day) VUV
    , LAST_VALUE(WST IGNORE NULLS) OVER(ORDER BY day) WST
    , LAST_VALUE(XAF IGNORE NULLS) OVER(ORDER BY day) XAF
    , LAST_VALUE(XAG IGNORE NULLS) OVER(ORDER BY day) XAG
    , LAST_VALUE(XAU IGNORE NULLS) OVER(ORDER BY day) XAU
    , LAST_VALUE(XCD IGNORE NULLS) OVER(ORDER BY day) XCD
    , LAST_VALUE(XDR IGNORE NULLS) OVER(ORDER BY day) XDR
    , LAST_VALUE(XOF IGNORE NULLS) OVER(ORDER BY day) XOF
    , LAST_VALUE(XPF IGNORE NULLS) OVER(ORDER BY day) XPF
    , LAST_VALUE(YER IGNORE NULLS) OVER(ORDER BY day) YER
    , LAST_VALUE(ZAR IGNORE NULLS) OVER(ORDER BY day) ZAR
    , LAST_VALUE(ZMK IGNORE NULLS) OVER(ORDER BY day) ZMK
    , LAST_VALUE(ZMW IGNORE NULLS) OVER(ORDER BY day) ZMW
    , LAST_VALUE(ZWL IGNORE NULLS) OVER(ORDER BY day) ZWL
      FROM days
      LEFT JOIN looker_table
      ON day = exchange_date
      order by exchange_date desc
      ),

     list_of_curs as (select "AFN"  as currency
      union all select "AMD"  as currency
      union all select "ANG"  as currency
      union all select "AOA"  as currency
      union all select "ARS"  as currency
      union all select "AUD"  as currency
      union all select "AWG"  as currency
      union all select "AZN"  as currency
      union all select "BAM"  as currency
      union all select "BBD"  as currency
      union all select "BDT"  as currency
      union all select "BGN"  as currency
      union all select "BHD"  as currency
      union all select "BIF"  as currency
      union all select "BMD"  as currency
      union all select "BND"  as currency
      union all select "BOB"  as currency
      union all select "BRL"  as currency
      union all select "BSD"  as currency
      union all select "BTC"  as currency
      union all select "BTN"  as currency
      union all select "BWP"  as currency
      union all select "BYN"  as currency
      union all select "BYR"  as currency
      union all select "BZD"  as currency
      union all select "CAD"  as currency
      union all select "CDF"  as currency
      union all select "CHF"  as currency
      union all select "CLF"  as currency
      union all select "CLP"  as currency
      union all select "CNY"  as currency
      union all select "COP"  as currency
      union all select "CRC"  as currency
      union all select "CUC"  as currency
      union all select "CUP"  as currency
      union all select "CVE"  as currency
      union all select "CZK"  as currency
      union all select "DJF"  as currency
      union all select "DKK"  as currency
      union all select "DOP"  as currency
      union all select "DZD"  as currency
      union all select "EGP"  as currency
      union all select "ERN"  as currency
      union all select "ETB"  as currency
      union all select "EUR"  as currency
      union all select "FJD"  as currency
      union all select "FKP"  as currency
      union all select "GBP"  as currency
      union all select "GEL"  as currency
      union all select "GGP"  as currency
      union all select "GHS"  as currency
      union all select "GIP"  as currency
      union all select "GMD"  as currency
      union all select "GNF"  as currency
      union all select "GTQ"  as currency
      union all select "GYD"  as currency
      union all select "HKD"  as currency
      union all select "HNL"  as currency
      union all select "HRK"  as currency
      union all select "HTG"  as currency
      union all select "HUF"  as currency
      union all select "IDR"  as currency
      union all select "ILS"  as currency
      union all select "IMP"  as currency
      union all select "INR"  as currency
      union all select "IQD"  as currency
      union all select "IRR"  as currency
      union all select "ISK"  as currency
      union all select "JEP"  as currency
      union all select "JMD"  as currency
      union all select "JOD"  as currency
      union all select "JPY"  as currency
      union all select "KES"  as currency
      union all select "KGS"  as currency
      union all select "KHR"  as currency
      union all select "KMF"  as currency
      union all select "KPW"  as currency
      union all select "KRW"  as currency
      union all select "KWD"  as currency
      union all select "KYD"  as currency
      union all select "KZT"  as currency
      union all select "LAK"  as currency
      union all select "LBP"  as currency
      union all select "LKR"  as currency
      union all select "LRD"  as currency
      union all select "LSL"  as currency
      union all select "LTL"  as currency
      union all select "LVL"  as currency
      union all select "LYD"  as currency
      union all select "MAD"  as currency
      union all select "MDL"  as currency
      union all select "MGA"  as currency
      union all select "MKD"  as currency
      union all select "MMK"  as currency
      union all select "MNT"  as currency
      union all select "MOP"  as currency
      union all select "MRO"  as currency
      union all select "MUR"  as currency
      union all select "MVR"  as currency
      union all select "MWK"  as currency
      union all select "MXN"  as currency
      union all select "MYR"  as currency
      union all select "MZN"  as currency
      union all select "NAD"  as currency
      union all select "NGN"  as currency
      union all select "NIO"  as currency
      union all select "NOK"  as currency
      union all select "NPR"  as currency
      union all select "NZD"  as currency
      union all select "OMR"  as currency
      union all select "PAB"  as currency
      union all select "PEN"  as currency
      union all select "PGK"  as currency
      union all select "PHP"  as currency
      union all select "PKR"  as currency
      union all select "PLN"  as currency
      union all select "PYG"  as currency
      union all select "QAR"  as currency
      union all select "RON"  as currency
      union all select "RSD"  as currency
      union all select "RUB"  as currency
      union all select "RWF"  as currency
      union all select "SAR"  as currency
      union all select "SBD"  as currency
      union all select "SCR"  as currency
      union all select "SDG"  as currency
      union all select "SEK"  as currency
      union all select "SGD"  as currency
      union all select "SHP"  as currency
      union all select "SLL"  as currency
      union all select "SOS"  as currency
      union all select "SRD"  as currency
      union all select "STD"  as currency
      union all select "SVC"  as currency
      union all select "SYP"  as currency
      union all select "SZL"  as currency
      union all select "THB"  as currency
      union all select "TJS"  as currency
      union all select "TMT"  as currency
      union all select "TND"  as currency
      union all select "TOP"  as currency
      union all select "TRY"  as currency
      union all select "TTD"  as currency
      union all select "TWD"  as currency
      union all select "TZS"  as currency
      union all select "UAH"  as currency
      union all select "UGX"  as currency
      union all select "USD"  as currency
      union all select "UYU"  as currency
      union all select "UZS"  as currency
      union all select "VEF"  as currency
      union all select "VND"  as currency
      union all select "VUV"  as currency
      union all select "WST"  as currency
      union all select "XAF"  as currency
      union all select "XAG"  as currency
      union all select "XAU"  as currency
      union all select "XCD"  as currency
      union all select "XDR"  as currency
      union all select "XOF"  as currency
      union all select "XPF"  as currency
      union all select "YER"  as currency
      union all select "ZAR"  as currency
      union all select "ZMK"  as currency
      union all select "ZMW"  as currency
      union all select "ZWL"  as currency),


      date_cur as (select * from days cross join list_of_curs)

      select timestamp(day) as day, currency, case currency
      when "AFN" then AFN
      when "AMD" then AMD
      when "ANG" then ANG
      when "AOA" then AOA
      when "ARS" then ARS
      when "AUD" then AUD
      when "AWG" then AWG
      when "AZN" then AZN
      when "BAM" then BAM
      when "BBD" then BBD
      when "BDT" then BDT
      when "BGN" then BGN
      when "BHD" then BHD
      when "BIF" then BIF
      when "BMD" then BMD
      when "BND" then BND
      when "BOB" then BOB
      when "BRL" then BRL
      when "BSD" then BSD
      when "BTC" then BTC
      when "BTN" then BTN
      when "BWP" then BWP
      when "BYN" then BYN
      when "BYR" then BYR
      when "BZD" then BZD
      when "CAD" then CAD
      when "CDF" then CDF
      when "CHF" then CHF
      when "CLF" then CLF
      when "CLP" then CLP
      when "CNY" then CNY
      when "COP" then COP
      when "CRC" then CRC
      when "CUC" then CUC
      when "CUP" then CUP
      when "CVE" then CVE
      when "CZK" then CZK
      when "DJF" then DJF
      when "DKK" then DKK
      when "DOP" then DOP
      when "DZD" then DZD
      when "EGP" then EGP
      when "ERN" then ERN
      when "ETB" then ETB
      when "EUR" then EUR
      when "FJD" then FJD
      when "FKP" then FKP
      when "GBP" then GBP
      when "GEL" then GEL
      when "GGP" then GGP
      when "GHS" then GHS
      when "GIP" then GIP
      when "GMD" then GMD
      when "GNF" then GNF
      when "GTQ" then GTQ
      when "GYD" then GYD
      when "HKD" then HKD
      when "HNL" then HNL
      when "HRK" then HRK
      when "HTG" then HTG
      when "HUF" then HUF
      when "IDR" then IDR
      when "ILS" then ILS
      when "IMP" then IMP
      when "INR" then INR
      when "IQD" then IQD
      when "IRR" then IRR
      when "ISK" then ISK
      when "JEP" then JEP
      when "JMD" then JMD
      when "JOD" then JOD
      when "JPY" then JPY
      when "KES" then KES
      when "KGS" then KGS
      when "KHR" then KHR
      when "KMF" then KMF
      when "KPW" then KPW
      when "KRW" then KRW
      when "KWD" then KWD
      when "KYD" then KYD
      when "KZT" then KZT
      when "LAK" then LAK
      when "LBP" then LBP
      when "LKR" then LKR
      when "LRD" then LRD
      when "LSL" then LSL
      when "LTL" then LTL
      when "LVL" then LVL
      when "LYD" then LYD
      when "MAD" then MAD
      when "MDL" then MDL
      when "MGA" then MGA
      when "MKD" then MKD
      when "MMK" then MMK
      when "MNT" then MNT
      when "MOP" then MOP
      when "MRO" then MRO
      when "MUR" then MUR
      when "MVR" then MVR
      when "MWK" then MWK
      when "MXN" then MXN
      when "MYR" then MYR
      when "MZN" then MZN
      when "NAD" then NAD
      when "NGN" then NGN
      when "NIO" then NIO
      when "NOK" then NOK
      when "NPR" then NPR
      when "NZD" then NZD
      when "OMR" then OMR
      when "PAB" then PAB
      when "PEN" then PEN
      when "PGK" then PGK
      when "PHP" then PHP
      when "PKR" then PKR
      when "PLN" then PLN
      when "PYG" then PYG
      when "QAR" then QAR
      when "RON" then RON
      when "RSD" then RSD
      when "RUB" then RUB
      when "RWF" then RWF
      when "SAR" then SAR
      when "SBD" then SBD
      when "SCR" then SCR
      when "SDG" then SDG
      when "SEK" then SEK
      when "SGD" then SGD
      when "SHP" then SHP
      when "SLL" then SLL
      when "SOS" then SOS
      when "SRD" then SRD
      when "STD" then STD
      when "SVC" then SVC
      when "SYP" then SYP
      when "SZL" then SZL
      when "THB" then THB
      when "TJS" then TJS
      when "TMT" then TMT
      when "TND" then TND
      when "TOP" then TOP
      when "TRY" then TRY
      when "TTD" then TTD
      when "TWD" then TWD
      when "TZS" then TZS
      when "UAH" then UAH
      when "UGX" then UGX
      when "USD" then USD
      when "UYU" then UYU
      when "UZS" then UZS
      when "VEF" then VEF
      when "VND" then VND
      when "VUV" then VUV
      when "WST" then WST
      when "XAF" then XAF
      when "XAG" then XAG
      when "XAU" then XAU
      when "XCD" then XCD
      when "XDR" then XDR
      when "XOF" then XOF
      when "XPF" then XPF
      when "YER" then YER
      when "ZAR" then ZAR
      when "ZMK" then ZMK
      when "ZMW" then ZMW
      when "ZWL" then ZWL
      end as rate from date_cur left join bq_filled on date_cur.day = bq_filled.exchange_date ;;


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
