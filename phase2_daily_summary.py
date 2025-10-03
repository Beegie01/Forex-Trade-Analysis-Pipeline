import copy
import pandas as pd
from helper_utils import HelperUtils as hu
import phase2_data_cleaning as ph2_dc
import os


def run_app(parent_folder_path: str=None,
            file_name="axi_trades.xlsx",
            acct_nr=4728846):

    # configure display settings for pandas output
    hu.configure_display_settings()

    # update cleaned dataset
    ph2_dc.run_app(parent_folder_path,
                   file_name=file_name)

    # extract cleaned data from csv file
    # you can change root_path value to the folder containing the custom scripts
    root_path = parent_folder_path
    sep = "/"
    read_fdr = "CleansedData"
    rfile_name = "axi_dataset_cleaned.csv"
    dest_path = sep.join([root_path, read_fdr, rfile_name])
    src_df = pd.read_csv(dest_path)
    # print(src_df.info())
    # print(src_df.head())
    # print(src_df.tail())
    df = copy.deepcopy(src_df)

    # recast columns to appropriate data types
    intg_cols = ["Ticket",
                 "Open Date Key",
                 "Close Date Key"]
    flg_cols = ["Size",
                "Open Price",
                "Close Price",
                "Commission",
                "Swap",
                "Profit"]
    date_cols = ["Open DateTime",
                 "Close DateTime",
                 "Open Date",
                 "Close Date"]
    recast_dtypes = hu.recast_dtypes(df,
                                     show_progress=False,
                                     date_cols=date_cols,
                                     int_cols=intg_cols,
                                     flt_cols=flg_cols)
    df = copy.deepcopy(recast_dtypes)

    # select account for data transformation
    cond = (df["Account Number"] == acct_nr)
    account_selected = df.loc[cond]
    df = copy.deepcopy(account_selected)
    # print(df.loc[df['Trade Closed'] == 0])

    # generate dim calendar for date
    min_date = df['Open Date'].min()
    dim_calendar = hu.generate_calendar(min_date,
                                        include_date_features=True)[['calendar_day',
                                                                     'date_key',
                                                                     'day_name']]
    # print(dim_calendar.info())
    # COMPUTE CORE KPIs & RATIOs
    # DAILY TRADE INFO
    # trade status
    col_list = ['Open Date Key',
                'Symbol',
                "Ticket"]
    daily_opened_trades = df[col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "trades_opened"})
    # col_list = ['Close Date Key', 'Symbol', "Ticket"]
    # daily_closed_trades = df[col_list].groupby(
    #     col_list[:-1]).count().reset_index().rename(
    #     columns={col_list[-1]: "trades_closed"})
    col_list = ["Open Date Key", "Symbol", "Trade Closed"]
    daily_closed_trades = df.loc[(df[col_list[-1]] == 1), col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "trades_closed"})
    daily_ongoing_trades = df.loc[(df[col_list[-1]] == 0), col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "trades_ongoing"})

    # trade wins
    col_list = ["Open Date Key", "Symbol", "Trade Won"]
    daily_trades_won = df.loc[(df[col_list[-1]] == 1), col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "trades_won"})
    daily_trades_lost = df.loc[(df[col_list[-1]] == -1), col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "trades_lost"})
    daily_trades_neutral = df.loc[(df[col_list[-1]] == 0),col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "trades_neutral"})

    # stop loss
    col_list = ["Open Date Key", "Symbol", "SL Triggered"]
    daily_stop_loss = df.loc[(df[col_list[-1]] == 1), col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "stop_loss"})

    # take profit
    col_list = ["Open Date Key", "Symbol", "TP Triggered"]
    daily_take_profit = df.loc[(df[col_list[-1]] == 1), col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "take_profit"})

    # net profit amount
    col_list = ["Open Date Key", "Symbol", "Net Profit"]
    # amount won
    cond = (df["Trade Won"] == 1)
    daily_amount_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "amount_won"})
    daily_avg_amount_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_amount_won"}).round(2)
    # amount lost
    cond = (df["Trade Won"] == -1)
    daily_amount_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "amount_lost"})
    daily_avg_amount_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_amount_lost"}).round(2)
    daily_net_profit = df[col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "net_profit"})
    daily_avg_net_profit = df[col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_net_profit"}).round(2)

    # return
    col_list = ["Open Date Key", "Symbol", "Return"]
    daily_avg_return = df[col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_return"}).round(4)
    daily_std_return = df[col_list].groupby(
        col_list[:-1]).std().reset_index().rename(
        columns={col_list[-1]: "std_return"}).round(4)


    # lot size
    col_list = ["Open Date Key", "Symbol", "Size"]
    cond = (df["Trade Won"] == 1)
    daily_lots_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "lots_won"})
    daily_avg_lots_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_lots_won"}).round(2)
    cond = (df["Trade Won"] == -1)
    daily_lots_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "lots_lost"})
    daily_avg_lots_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_lots_lost"}).round(2)

    # pip size
    col_list = ["Open Date Key", "Symbol", "Pips Gained"]
    cond = (df["Trade Won"] == 1)
    daily_pips_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "pips_won"})
    daily_avg_pips_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_pips_won"}).round(2)
    cond = (df["Trade Won"] == -1)
    daily_pips_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "pips_lost"})
    daily_avg_pips_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_pips_lost"}).round(2)

    # minutes spent
    col_list = ["Open Date Key", "Symbol", "Trade Duration (minutes)"]
    cond = (df["Trade Won"] == 1)
    daily_minutes_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "minutes_spent_won"}).round(0)
    daily_avg_minutes_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_minutes_spent_won"}).round(0)
    cond = (df["Trade Won"] == -1)
    daily_minutes_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "minutes_spent_lost"}).round(0)
    daily_avg_minutes_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_minutes_spent_lost"}).round(0)

    # scalping vs overnight trades
    # won vs lost
    col_list = ["Open Date Key",
                "Symbol",
                "Trade Scalping",
                "Overnight Trade"]
    cond = (df["Trade Won"] == 1)
    daily_scalping_overnight_won = df.loc[cond, col_list].groupby(
        col_list[:-2]).sum().reset_index().rename(
        columns={col_list[-2]: "trade_scalping_won",
                 col_list[-1]: "overnight_trade_won"}).round(0)
    cond = (df["Trade Won"] == -1)
    daily_scalping_overnight_lost = df.loc[cond, col_list].groupby(
        col_list[:-2]).sum().reset_index().rename(
        columns={col_list[-2]: "trade_scalping_lost",
                 col_list[-1]: "overnight_trade_lost"}).round(0)
    # amount won vs lost
    col_list = ["Open Date Key",
                "Symbol",
                "Net Profit"]
    # scalping
    cond = ((df["Trade Won"] == 1) & (df["Trade Scalping"] == 1))
    daily_scalping_amount_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "scalping_amount_won"}).round(0)
    cond = ((df["Trade Won"] == -1) & (df["Trade Scalping"] == 1))
    daily_scalping_amount_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "scalping_amount_lost"}).round(0)
    # overnight
    cond = ((df["Trade Won"] == 1) & (df["Overnight Trade"] == 1))
    daily_overnight_amount_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "overnight_amount_won"}).round(0)
    cond = ((df["Trade Won"] == -1) & (df["Overnight Trade"] == 1))
    daily_overnight_amount_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "overnight_amount_lost"}).round(0)

    # max profit and loss
    col_list = ["Open Date Key",
                "Symbol",
                "Profit"]
    cond = (df["Trade Won"] == 1)
    daily_max_profit = df.loc[cond, col_list].groupby(
        col_list[:-1]).agg(['max']).reset_index()
    daily_max_profit.columns = [''.join(col).strip() for col in daily_max_profit.columns.values]
    daily_max_profit = daily_max_profit.rename(columns={"Profitmax":"max_profit"})
    cond = (df["Trade Won"] == -1)
    daily_max_loss = df.loc[cond, col_list].groupby(
        col_list[:-1]).agg(['min']).reset_index()
    daily_max_loss.columns = [''.join(col).strip() for col in daily_max_loss.columns.values]
    daily_max_loss = daily_max_loss.rename(columns={"Profitmin": "max_loss"})


    # merge with calendar table
    daily_agg = pd.merge(dim_calendar,
                         daily_opened_trades,
                         left_on='date_key',
                         right_on='Open Date Key',
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_closed_trades,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_ongoing_trades,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_trades_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_trades_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_trades_neutral,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_stop_loss,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_take_profit,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_amount_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_amount_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_amount_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_amount_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_net_profit,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_net_profit,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_return,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_std_return,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_lots_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_lots_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_lots_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_lots_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_pips_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_pips_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_pips_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_pips_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_minutes_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_minutes_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_minutes_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_minutes_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_scalping_overnight_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_scalping_overnight_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_scalping_amount_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_scalping_amount_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_overnight_amount_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_overnight_amount_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_max_profit,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_max_loss,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = daily_agg.fillna(0)

    # convert header all lowercase
    daily_agg.columns = list(pd.Series(daily_agg.columns).str.lower())
    final_agg_df = copy.deepcopy(daily_agg)
    # print(final_agg_df.head())
    # print(final_agg_df.tail())

    # compute daily net profit
    col_list = ["Open Date Key",
                "Profit",
                "Net Profit"]
    main_daily_net_profit = df[col_list].groupby(
        col_list[0]).sum().reset_index().rename(
        columns={col_list[1]:"profit",
                 col_list[-1]: "net_profit"}).round(4)
    # print(main_daily_net_profit)

    # cumulative net profit
    main_daily_net_profit["cumulative_profit"] = main_daily_net_profit["net_profit"].cumsum().round(4)
    # print(main_daily_net_profit)

    # compute daily return
    col_list = ["Open Date Key", "Return"]
    main_daily_avg_return = df[col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_return"}).round(4)
    main_daily_std_return = df[col_list].groupby(
        col_list[:-1]).std().reset_index().rename(
        columns={col_list[-1]: "std_return"}).round(4)

    # std of negative returns
    col_list = ["Open Date Key", "Return"]
    cond = (df["Trade Won"] == -1)
    main_daily_nstd_return = df.loc[cond, col_list].groupby(
        col_list[:-1]).std().reset_index().rename(
        columns={col_list[-1]: "std_neg_return"}).round(4)

    # compute daily trade duration, scalping, overnight trades, and pips gained
    col_list = ["Open Date Key",
                "Trade Duration (minutes)",
                "Trade Scalping",
                "Overnight Trade",
                "Size",
                "Pips Gained"]
    main_daily_pips_duration = df[col_list].groupby(
        col_list[0]).sum().reset_index().rename(
        columns={col_list[1]: "minutes_spent",
                 col_list[2]: "trade_scalping",
                 col_list[3]: "overnight_trade",
                 col_list[4]: "traded_lots",
                 col_list[-1]: "pips_gained"}).round(4)

    col_list = ["Open Date Key",
                "Profit"]
    main_daily_max_profit_loss = df[col_list].groupby(
        col_list[0]).agg(['max', 'min']).reset_index()
    main_daily_max_profit_loss.columns = [''.join(col).strip() for col in main_daily_max_profit_loss.columns.values]
    main_daily_max_profit_loss = main_daily_max_profit_loss.rename(
        columns={"Profitmax":"max_profit",
                 "Profitmin": "max_loss"})

    # merge return columns
    main_daily_agg = pd.merge(dim_calendar,
                         main_daily_avg_return,
                         left_on=['date_key'],
                         right_on=['Open Date Key'],
                         how='left').drop(columns='Open Date Key')
    main_daily_agg = pd.merge(main_daily_agg,
                         main_daily_std_return,
                         left_on=['date_key'],
                         right_on=['Open Date Key'],
                         how='left').drop(columns='Open Date Key')
    main_daily_agg = pd.merge(main_daily_agg,
                              main_daily_nstd_return,
                              left_on=['date_key'],
                              right_on=['Open Date Key'],
                              how='left').drop(columns='Open Date Key')
    main_daily_agg = pd.merge(main_daily_agg,
                              main_daily_net_profit,
                              left_on=['date_key'],
                              right_on=['Open Date Key'],
                              how='left').drop(columns='Open Date Key')
    main_daily_agg = pd.merge(main_daily_agg,
                              main_daily_pips_duration,
                              left_on=['date_key'],
                              right_on=['Open Date Key'],
                              how='left').drop(columns='Open Date Key')
    main_daily_agg = pd.merge(main_daily_agg,
                              main_daily_max_profit_loss,
                              left_on=['date_key'],
                              right_on=['Open Date Key'],
                              how='left').drop(columns='Open Date Key')
    main_daily_agg = main_daily_agg.fillna(0)

    # print(main_daily_agg.head())
    # print(main_daily_agg.tail())

    # create folder if it does not exist
    read_fdr = "TransformedData"
    dest_path = sep.join([root_path, read_fdr])
    os.makedirs(dest_path, exist_ok=True)

    # write data to filesystem
    wfile_name = "axi_dataset_transformed_daily.csv"
    dest_path = sep.join([root_path, read_fdr, wfile_name])
    final_agg_df.to_csv(path_or_buf=dest_path,
              index=False,
              encoding='utf8')

    wfile_name = "axi_dataset_transformed_daily_main.csv"
    dest_path = sep.join([root_path, read_fdr, wfile_name])
    main_daily_agg.to_csv(path_or_buf=dest_path,
              index=False,
              encoding='utf8')
    print("\nDAILY SUMMARY CUBE DEVELOPMENT FINISH!\n")
