-- View: gold.trade_history

-- DROP VIEW gold.trade_history;

CREATE OR REPLACE VIEW gold.trade_history
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
   FROM core.trade_history;

ALTER TABLE gold.trade_history
    OWNER TO postgres;

-- View: gold.dim_account_info

-- DROP VIEW gold.dim_account_info;

CREATE OR REPLACE VIEW gold.dim_account_info
 AS
 SELECT account_number,
    account_name,
    account_currency,
    currency_name
   FROM core.dim_account_info;

ALTER TABLE gold.dim_account_info
    OWNER TO postgres;

-- View: gold.dim_date

-- DROP VIEW gold.dim_date;

CREATE OR REPLACE VIEW gold.dim_date
 AS
 SELECT cal_date,
    date_key,
    cal_year,
    cal_quarter,
    quarter_name,
	quarter_ending,
    cal_month,
    month_name,
    month_start,
    month_end,
	week_year_num,
    week_of_year,
	week_month_num,
    week_of_month,
	week_day_num,
    cal_day,
    day_name,
    is_business_day
   FROM core.dim_date;

ALTER TABLE gold.dim_date
    OWNER TO postgres;

-- View: gold.dim_clock

-- DROP VIEW gold.dim_clock;

CREATE OR REPLACE VIEW gold.dim_clock
 AS
 SELECT clock,
    clock_hour,
    clock_minute,
    clock_second,
    is_working_hour,
    is_daytime,
    part_of_day,
    part_of_day_num
   FROM core.dim_clock;

ALTER TABLE gold.dim_clock
    OWNER TO postgres;

SELECT 'OPERATION COMPLETE!!'
