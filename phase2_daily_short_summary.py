import copy
import numpy as np
import pandas as pd
from helper_utils import HelperUtils as hu
import phase2_data_cleaning as ph2_dc
import os


def run_app(run_dialog=True,
            acct_nr=4728846):

    # configure display settings for pandas output
    hu.configure_display_settings()

    # update cleaned dataset
    ph2_dc.run_app(run_dialog)

    # extract cleaned data from csv file
    # you can change root_path value to the folder containing the custom scripts
    root_path = "/Users/osagieaib/Library/CloudStorage/OneDrive-GodsVisionEnterprise/Documents/Workspace/IT Career/Cedarstone"
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
    # DAILY TRADE INFO - SHORT
    # short
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = (df[col_list[-1]] == "sell")
    daily_shorts_opened = df.loc[cond, col_list].groupby(col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "shorts_opened"})
    col_list = ["Close Date Key", "Symbol", "Type"]
    daily_shorts_closed = df.loc[cond, col_list].groupby(col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "shorts_closed"})
    # ongoing and short
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = ((df["Trade Closed"] == 0) & (df[col_list[-1]] == "sell"))
    daily_shorts_ongoing = df.loc[cond, col_list].groupby(col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "shorts_ongoing"})
    #
    # # won and short
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = (df["Trade Won"] == 1) & (df[col_list[-1]] == "sell")
    daily_shorts_won = df.loc[cond, col_list].groupby(col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "shorts_won"})

    # # lost and short
    cond = (df["Trade Won"] == -1) & (df[col_list[-1]] == "sell")
    daily_shorts_lost = df.loc[cond, col_list].groupby(col_list[:-1]).count().reset_index().rename(
            columns={col_list[-1]: "shorts_lost"})
    #
    # # neutral and short
    cond = (df["Trade Won"] == 0) & (df[col_list[-1]] == "sell")
    daily_shorts_neutral = df.loc[cond, col_list].groupby(col_list[:-1]).count().reset_index().rename(
            columns={col_list[-1]: "shorts_neutral"})
    #
    # # short stop loss
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = ((df["SL Triggered"] == 1) & (df[col_list[-1]] =="sell"))
    daily_short_sl = df.loc[cond, col_list].groupby(col_list[:-1]).count().reset_index().rename(
            columns={col_list[-1]: "short_stop_loss"})

    # short stop loss
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = ((df["TP Triggered"] == 1) & (df[col_list[-1]] == "sell"))
    daily_short_tp = df.loc[cond, col_list].groupby(col_list[:-1]).count().reset_index().rename(
            columns={col_list[-1]: "short_take_profit"})

    # won and short
    col_list = ["Open Date Key", "Symbol", "Net Profit"]
    cond = ((df["Trade Won"] == 1) & (df["Type"] == "sell"))
    daily_short_amount_won = df.loc[cond, col_list].groupby(col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "short_amount_won"})
    daily_avg_short_amount_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_amount_won"}).round(2)

    # lost and short
    cond = ((df["Trade Won"] == -1) & (df["Type"] == "sell"))
    daily_short_amount_lost = df.loc[cond, col_list].groupby(col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "short_amount_lost"})
    daily_avg_short_amount_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_amount_lost"}).round(2)
    cond = (df["Type"] == "sell")
    daily_short_net_profit = df.loc[cond, col_list].groupby(col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "short_net_profit"})
    daily_avg_short_net_profit = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_net_profit"}).round(2)

    # short return
    col_list = ["Open Date Key", "Symbol", "Return"]
    cond = (df["Type"] == "sell")
    daily_avg_short_return = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_return"}).round(4)
    daily_std_short_return = df.loc[cond, col_list].groupby(
        col_list[:-1]).std().reset_index().rename(
        columns={col_list[-1]: "std_short_return"}).round(4)

    # short lot size
    col_list = ["Open Date Key", "Symbol", "Size"]
    cond = ((df["Trade Won"] == 1) & (df["Type"] == "sell"))
    daily_short_lots_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "short_lots_won"})
    daily_avg_short_lots_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_lots_won"}).round(2)
    cond = ((df["Trade Won"] == -1) & (df["Type"] == "sell"))
    daily_short_lots_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "short_lots_lost"})
    daily_avg_short_lots_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_lots_lost"}).round(2)

    # short pips
    col_list = ["Open Date Key", "Symbol", "Pips Gained"]
    cond = ((df["Trade Won"] == 1) & (df["Type"] == "sell"))
    daily_short_pips_won = df.loc[cond, col_list].groupby(col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "short_pips_won"})
    daily_avg_short_pips_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_pips_won"}).round(2)
    cond = ((df["Trade Won"] == -1) & (df["Type"] == "sell"))
    daily_short_pips_lost = df.loc[cond, col_list].groupby(col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "short_pips_lost"})
    daily_avg_short_pips_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_pips_lost"}).round(2)

    # minutes spent
    col_list = ["Open Date Key", "Symbol", "Trade Duration (minutes)"]
    cond = ((df["Trade Won"] == 1) & (df["Type"] == "sell"))
    daily_short_minutes_won = df.loc[cond, col_list].groupby(col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "short_minutes_spent_won"}).round(0)
    daily_avg_short_minutes_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_minutes_spent_won"}).round(0)
    cond = ((df["Trade Won"] == -1) & (df["Type"] == "sell"))
    daily_short_minutes_lost = df.loc[cond, col_list].groupby(col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "short_minutes_spent_lost"}).round(0)
    daily_avg_short_minutes_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_short_minutes_spent_lost"}).round(0)
    # closing balance daily

    # merge with calendar table
    daily_agg = pd.merge(dim_calendar,
                         daily_shorts_opened,
                         left_on='date_key',
                         right_on='Open Date Key',
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_shorts_closed,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Close Date Key', 'Symbol'],
                         how='left').drop(columns='Close Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_shorts_ongoing,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_shorts_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_shorts_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_shorts_neutral,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_sl,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_tp,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_amount_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_amount_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_amount_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_amount_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_net_profit,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_net_profit,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_return,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_std_short_return,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_lots_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_lots_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_lots_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_lots_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_pips_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_pips_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_pips_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_pips_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_minutes_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_minutes_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_short_minutes_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_short_minutes_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = daily_agg.fillna(0)

    # convert header all lowercase
    daily_agg.columns = list(pd.Series(daily_agg.columns).str.lower())
    # print(daily_agg.head())
    # print(daily_agg.tail())

    # create folder if it does not exist
    read_fdr = "TransformedData"
    dest_path = sep.join([root_path, read_fdr])
    os.makedirs(dest_path, exist_ok=True)

    # write data to filesystem
    wfile_name = "axi_dataset_transformed_short.csv"
    dest_path = sep.join([root_path, read_fdr, wfile_name])
    daily_agg.to_csv(path_or_buf=dest_path,
              index=False,
              encoding='utf8')
    print("\nSHORT SUMMARY CUBE DEVELOPMENT FINISH!\n")