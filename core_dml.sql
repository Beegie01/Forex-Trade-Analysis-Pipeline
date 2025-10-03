-- schema: core
DROP SCHEMA IF EXISTS core CASCADE;

CREATE SCHEMA core;

-- View: core.trade_hist_info

-- DROP MATERIALIZED VIEW IF EXISTS core.trade_hist_info;

CREATE MATERIALIZED VIEW IF NOT EXISTS core.trade_hist_info
TABLESPACE pg_default
AS
 WITH par_table AS (
         SELECT ath.account_nr,
            ath.ticket,
            ath.open_datetime,
            ath.close_datetime,
            ath.open_date_key,
            ath.close_date_key,
            ath.type,
            ath.symbol,
            ath.size,
            ath.open_price,
            ath.close_price,
            ath.commission,
            ath.swap,
            ath.profit,
            ath.stop_loss,
            ath.take_profit,
            round(EXTRACT(epoch FROM ath.close_datetime - ath.open_datetime) / 60::numeric, 1) AS minutes_spent,
                CASE
                    WHEN ath.type::text = 'buy'::text THEN (ath.close_price - ath.open_price) * 10000::numeric
                    WHEN ath.type::text = 'sell'::text THEN (ath.open_price - ath.close_price) * 10000::numeric
                    ELSE NULL::numeric
                END AS pips_gained,
            ath.profit - ath.commission - ath.swap AS net_profit,
                CASE
                    WHEN ath.open_datetime IS NOT NULL AND ath.close_datetime IS NOT NULL THEN 1
                    ELSE 0
                END AS trade_closed
           FROM staging.axi_trade_history ath
        ), step2 AS (
         SELECT pta.account_nr,
            pta.ticket,
            pta.open_datetime,
            pta.close_datetime,
            pta.open_date_key,
            pta.close_date_key,
            pta.type,
            pta.symbol,
            pta.size,
            pta.open_price,
            pta.close_price,
            pta.commission,
            pta.swap,
            pta.profit,
            pta.stop_loss,
            pta.take_profit,
            pta.minutes_spent,
            pta.pips_gained,
            pta.net_profit,
            pta.trade_closed,
                CASE
                    WHEN pta.trade_closed = 1 AND pta.net_profit > 0::numeric THEN 1
                    WHEN pta.trade_closed = 1 AND pta.net_profit < 0::numeric THEN '-1'::integer
                    WHEN pta.trade_closed = 1 AND pta.net_profit = 0::numeric THEN 0
                    ELSE 9999
                END AS trade_won,
            sum(pta.net_profit) OVER (PARTITION BY pta.account_nr ORDER BY pta.account_nr, pta.open_datetime) AS cumulative_profit,
            sum(pta.profit - COALESCE(pta.commission, 0::numeric) - COALESCE(pta.swap, 0::numeric)) OVER (PARTITION BY pta.account_nr ORDER BY pta.account_nr, pta.open_datetime) AS balance
           FROM par_table pta
        ), step3 AS (
         SELECT s2.account_nr,
            s2.ticket,
            s2.open_datetime,
            s2.close_datetime,
            s2.open_date_key,
            s2.close_date_key,
            s2.type,
            s2.symbol,
            s2.size,
            s2.open_price,
            s2.close_price,
            s2.commission,
            s2.swap,
            s2.profit,
            s2.stop_loss,
            s2.take_profit,
            s2.minutes_spent,
            s2.pips_gained,
            s2.net_profit,
            s2.trade_closed,
            s2.trade_won,
            s2.cumulative_profit,
            s2.balance,
            lag(s2.balance, 1) OVER (PARTITION BY s2.account_nr ORDER BY s2.account_nr, s2.open_datetime) AS initial_balance,
                CASE
                    WHEN lag(s2.trade_won, 0) OVER (PARTITION BY s2.account_nr ORDER BY s2.account_nr, s2.open_datetime) = '-1'::integer AND lag(s2.trade_won, 1) OVER (PARTITION BY s2.account_nr ORDER BY s2.account_nr, s2.open_datetime) = '-1'::integer AND lag(s2.trade_won, 2) OVER (PARTITION BY s2.account_nr ORDER BY s2.account_nr, s2.open_datetime) = '-1'::integer THEN 1
                    ELSE 0
                END AS three_consecutive_losses
           FROM step2 s2
        )
 SELECT account_nr,
    ticket,
    open_date_key,
    close_date_key,
	TO_CHAR(open_datetime, 'hh24:mi:ss')::time open_time,
	TO_CHAR(close_datetime, 'hh24:mi:ss')::time close_time,
    type,
    symbol,
    size,
    open_price,
    close_price,
    commission,
    swap,
    profit,
    stop_loss,
    take_profit,
    minutes_spent,
    pips_gained,
    net_profit,
    trade_closed,
    trade_won,
    cumulative_profit,
    balance,
    initial_balance,
    three_consecutive_losses,
    round(net_profit / initial_balance, 4) AS trade_return,
        CASE
            WHEN stop_loss = close_price THEN 1
            ELSE 0
        END AS sl_triggered,
        CASE
            WHEN take_profit = close_price THEN 1
            ELSE 0
        END AS tp_triggered,
        CASE
            WHEN trade_closed = 1 AND minutes_spent < 2::numeric THEN 1
            ELSE 0
        END AS trade_scalping,
        CASE
            WHEN trade_closed = 1 AND round(minutes_spent / 60::numeric, 0) > 8::numeric THEN 1
            ELSE 0
        END AS overnight_trade
   FROM step3 s3
WITH DATA;

ALTER TABLE IF EXISTS core.trade_hist_info
    OWNER TO postgres;

-- View: core.trade_history

-- DROP VIEW core.trade_history;

CREATE OR REPLACE VIEW core.trade_history
 AS
 SELECT account_nr,
    ticket,
    open_date_key,
    close_date_key,
    open_time,
    close_time,
    type,
    symbol,
    size,
    open_price,
    close_price,
    commission,
    swap,
    profit,
    stop_loss,
    take_profit,
    minutes_spent,
    pips_gained,
    net_profit,
    trade_closed,
    trade_won,
    cumulative_profit,
    balance,
    initial_balance,
    three_consecutive_losses,
    trade_return,
    sl_triggered,
    tp_triggered,
    trade_scalping,
    overnight_trade
   FROM core.trade_hist_info
  WHERE type::text = ANY (ARRAY['buy'::character varying, 'sell'::character varying]::text[]);

ALTER TABLE core.trade_history
    OWNER TO postgres;

-- View: core.dim_account_info

CREATE OR REPLACE VIEW core.dim_account_info
AS
 SELECT account_number,
    account_name,
    account_currency,
        CASE
            WHEN account_currency::text = 'GBP'::text THEN 'POUND STERLING'::text
            WHEN account_currency::text = 'USD'::text THEN 'US DOLLARS'::text
            WHEN account_currency::text = 'CAD'::text THEN 'CANADIAN DOLLARS'::text
            ELSE NULL::text
        END AS currency_name
   FROM staging.dim_account;

ALTER TABLE core.dim_account_info
    OWNER TO postgres;

-- View: core.dim_date

-- DROP VIEW core.dim_date;

CREATE OR REPLACE VIEW core.dim_date
 AS
 WITH mdate AS (
         SELECT min(ath.open_datetime)::date AS min_date
           FROM staging.axi_trade_history ath
        ), cal_date AS (
         SELECT to_char(s.a, 'YYYY-MM-DD'::text)::date AS cdate
           FROM generate_series((( SELECT mdate.min_date
                   FROM mdate))::timestamp with time zone, now()::date::timestamp with time zone, '1 day'::interval) s(a)
        ), main_cal AS (
         SELECT cdy.cdate,
            to_char(cdy.cdate::timestamp with time zone, 'yyyymmdd'::text)::integer AS date_key,
            to_char(cdy.cdate::timestamp with time zone, 'yyyy'::text)::integer AS cal_year,
            to_char(cdy.cdate::timestamp with time zone, 'q'::text)::integer AS cal_quarter,
            concat('q'::text, to_char(cdy.cdate::timestamp with time zone, 'q'::text)) AS quarter_name,
            to_char(cdy.cdate::timestamp with time zone, 'mm'::text)::integer AS cal_month,
            to_char(cdy.cdate::timestamp with time zone, 'Mon'::text) AS month_name,
            date_trunc('month'::text, cdy.cdate::timestamp with time zone)::date AS month_start,
            (date_trunc('month'::text, cdy.cdate::timestamp with time zone) + '1 mon -1 days'::interval)::date AS month_end,
            EXTRACT(week FROM cdy.cdate) AS week_of_year,
            (EXTRACT(day FROM cdy.cdate)::integer - 1) / 7 + 1 AS week_of_month,
            to_char(cdy.cdate::timestamp with time zone, 'dd'::text)::integer AS cal_day,
            "left"(to_char(cdy.cdate::timestamp with time zone, 'Day'::text), 3) AS day_name
           FROM cal_date cdy
        )
 SELECT cdate AS cal_date,
    date_key,
    cal_year,
    cal_quarter,
    quarter_name,
		CASE
			WHEN cal_quarter = 1 THEN concat('03', cal_year)
			WHEN cal_quarter = 2 THEN concat('06', cal_year)
			WHEN cal_quarter = 3 THEN concat('09', cal_year)
			WHEN cal_quarter = 4 THEN concat('12', cal_year)
			ELSE NULL::text
		END AS quarter_ending,
    cal_month,
    month_name,
    month_start,
    month_end,
	week_of_year week_year_num,
    concat('wk'::text, week_of_year) week_of_year,
	week_of_month week_month_num,
    concat('wk'::text, week_of_month) week_of_month,
        CASE
            WHEN day_name = 'Sun'::text THEN 1
            WHEN day_name = 'Mon'::text THEN 2
            WHEN day_name = 'Tue'::text THEN 3
            WHEN day_name = 'Wed'::text THEN 4
            WHEN day_name = 'Thu'::text THEN 5
            WHEN day_name = 'Fri'::text THEN 6
            WHEN day_name = 'Sat'::text THEN 7
            ELSE NULL::integer
        END AS week_day_num,
    cal_day,
    day_name,
        CASE
            WHEN day_name = ANY (ARRAY['Sat'::text, 'Sun'::text]) THEN 0
            ELSE 1
        END AS is_business_day
   FROM main_cal mcl;

ALTER TABLE core.dim_date
    OWNER TO postgres;

-- View: core.dim_clock

-- DROP VIEW core.dim_clock;

CREATE OR REPLACE VIEW core.dim_clock
 AS
 WITH all_sec AS (
         SELECT gs.gs::time without time zone AS clock
           FROM generate_series('2025-01-01 00:00:00'::timestamp without time zone, '2025-01-01 23:59:59'::timestamp without time zone, '00:00:01'::interval) gs(gs)
        ), main_attr AS (
         SELECT all_sec.clock,
            to_char(all_sec.clock::interval, 'hh24'::text) AS clock_hour,
            to_char(all_sec.clock::interval, 'mi'::text) AS clock_minute,
            to_char(all_sec.clock::interval, 'ss'::text) AS clock_second,
                CASE
                    WHEN all_sec.clock >= '09:00:00'::time without time zone AND all_sec.clock <= '17:00:00'::time without time zone THEN 1
                    ELSE 0
                END AS is_working_hour,
                CASE
                    WHEN all_sec.clock >= '06:00:00'::time without time zone AND all_sec.clock <= '18:59:59'::time without time zone THEN 1
                    ELSE 0
                END AS is_daytime,
                CASE
                    WHEN all_sec.clock >= '00:00:00'::time without time zone AND all_sec.clock <= '05:59:59'::time without time zone THEN 'night'::text
                    WHEN all_sec.clock >= '06:00:00'::time without time zone AND all_sec.clock <= '11:59:59'::time without time zone THEN 'morning'::text
                    WHEN all_sec.clock >= '12:00:00'::time without time zone AND all_sec.clock <= '16:59:59'::time without time zone THEN 'afternoon'::text
                    WHEN all_sec.clock >= '17:00:00'::time without time zone AND all_sec.clock <= '20:59:59'::time without time zone THEN 'evening'::text
                    WHEN all_sec.clock >= '21:00:00'::time without time zone AND all_sec.clock <= '23:59:59'::time without time zone THEN 'night'::text
                    ELSE NULL::text
                END AS part_of_day
           FROM all_sec
        )
 SELECT clock,
    clock_hour,
    clock_minute,
    clock_second,
    is_working_hour,
    is_daytime,
    part_of_day,
        CASE
            WHEN part_of_day = 'morning'::text THEN 1
            WHEN part_of_day = 'afternoo'::text THEN 2
            WHEN part_of_day = 'evening'::text THEN 3
            WHEN part_of_day = 'night'::text THEN 4
            ELSE NULL::integer
        END AS part_of_day_num
   FROM main_attr mat;

ALTER TABLE core.dim_clock
    OWNER TO postgres;

SELECT 'OPERATION COMPLETE!!'