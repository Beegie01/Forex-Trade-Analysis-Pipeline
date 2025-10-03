import copy
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
    # DAILY TRADE INFO - LONG
    # long
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = (df[col_list[-1]] == "buy")
    daily_longs_opened = df.loc[cond, col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "longs_opened"})
    col_list = ["Close Date Key", "Symbol", "Type"]
    daily_longs_closed = df.loc[cond, col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "longs_closed"})

    # ongoing and long
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = ((df["Trade Closed"] == 0) & (df[col_list[-1]] == "buy"))
    daily_longs_ongoing = df.loc[cond, col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "longs_ongoing"})

    # won and long
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = (df["Trade Won"] == 1) & (df[col_list[-1]] == "buy")
    daily_longs_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "longs_won"})

    # lost and long
    cond = (df["Trade Won"] == -1) & (df[col_list[-1]] == "buy")
    daily_longs_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "longs_lost"})

    # neutral and long
    cond = (df["Trade Won"] == 0) & (df[col_list[-1]] == "buy")
    daily_longs_neutral = df.loc[cond, col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "longs_neutral"})

    # long stop loss
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = ((df["SL Triggered"] == 1) & (df[col_list[-1]] =="buy"))
    daily_long_sl = df.loc[cond, col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "long_stop_loss"})

    # long stop loss
    col_list = ["Open Date Key", "Symbol", "Type"]
    cond = ((df["TP Triggered"] == 1) & (df[col_list[-1]] == "buy"))
    daily_long_tp = df.loc[cond, col_list].groupby(
        col_list[:-1]).count().reset_index().rename(
        columns={col_list[-1]: "long_take_profit"})

    # won and long
    col_list = ["Open Date Key", "Symbol", "Net Profit"]
    cond = ((df["Trade Won"] == 1) & (df["Type"] == "buy"))
    daily_long_amount_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "long_amount_won"})
    daily_avg_long_amount_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_amount_won"}).round(2)

    # lost and long
    cond = ((df["Trade Won"] == -1) & (df["Type"] == "buy"))
    daily_long_amount_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "long_amount_lost"})
    daily_avg_long_amount_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_amount_lost"}).round(2)
    cond = (df["Type"] == "buy")
    daily_long_net_profit = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "long_net_profit"})
    daily_avg_long_net_profit = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_net_profit"}).round(2)

    # long return
    col_list = ["Open Date Key", "Symbol", "Return"]
    cond = (df["Type"] == "buy")
    daily_avg_long_return = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_return"}).round(4)
    daily_std_long_return = df.loc[cond, col_list].groupby(
        col_list[:-1]).std().reset_index().rename(
        columns={col_list[-1]: "std_long_return"}).round(4)

    # long lot size
    col_list = ["Open Date Key", "Symbol", "Size"]
    cond = ((df["Trade Won"] == 1) & (df["Type"] == "buy"))
    daily_long_lots_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "long_lots_won"})
    daily_avg_long_lots_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_lots_won"}).round(2)
    cond = ((df["Trade Won"] == -1) & (df["Type"] == "buy"))
    daily_long_lots_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "long_lots_lost"})
    daily_avg_long_lots_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_lots_lost"}).round(2)

    # long pips
    col_list = ["Open Date Key", "Symbol", "Pips Gained"]
    cond = ((df["Trade Won"] == 1) & (df["Type"] == "buy"))
    daily_long_pips_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "long_pips_won"})
    daily_avg_long_pips_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_pips_won"}).round(2)
    cond = ((df["Trade Won"] == -1) & (df["Type"] == "buy"))
    daily_long_pips_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "long_pips_lost"})
    daily_avg_long_pips_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_pips_lost"}).round(2)

    # minutes spent
    col_list = ["Open Date Key", "Symbol", "Trade Duration (minutes)"]
    cond = ((df["Trade Won"] == 1) & (df["Type"] == "buy"))
    daily_long_minutes_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "long_minutes_spent_won"}).round(0)
    daily_avg_long_minutes_won = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_minutes_spent_won"}).round(0)
    cond = ((df["Trade Won"] == -1) & (df["Type"] == "buy"))
    daily_long_minutes_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).sum().reset_index().rename(
        columns={col_list[-1]: "long_minutes_spent_lost"}).round(0)
    daily_avg_long_minutes_lost = df.loc[cond, col_list].groupby(
        col_list[:-1]).mean().reset_index().rename(
        columns={col_list[-1]: "avg_long_minutes_spent_lost"}).round(0)
    # closing balance daily

    # merge with calendar table
    daily_agg = pd.merge(dim_calendar,
                         daily_longs_opened,
                         left_on='date_key',
                         right_on='Open Date Key',
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_longs_closed,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Close Date Key', 'Symbol'],
                         how='left').drop(columns='Close Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_longs_ongoing,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_longs_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_longs_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_longs_neutral,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_sl,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_tp,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_amount_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_amount_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_amount_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_amount_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_net_profit,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_net_profit,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_return,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_std_long_return,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_lots_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_lots_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_lots_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_lots_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_pips_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_pips_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_pips_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_pips_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_minutes_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_minutes_won,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_long_minutes_lost,
                         left_on=['date_key', 'Symbol'],
                         right_on=['Open Date Key', 'Symbol'],
                         how='left').drop(columns='Open Date Key')
    daily_agg = pd.merge(daily_agg,
                         daily_avg_long_minutes_lost,
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
    wfile_name = "axi_dataset_transformed_long.csv"
    dest_path = sep.join([root_path, read_fdr, wfile_name])
    daily_agg.to_csv(path_or_buf=dest_path,
              index=False,
              encoding='utf8')
    print("\nLONG SUMMARY CUBE DEVELOPMENT FINISH!\n")
