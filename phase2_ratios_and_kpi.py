import copy
import numpy as np
import pandas as pd
from sympy.codegen import Print

from helper_utils import HelperUtils as hu
import phase2_data_cleaning as ph2_dc
import datetime


def run_app(acct_nr: int = 4728846):

    # configure display settings for pandas output
    hu.configure_display_settings()

    # extract cleaned data from csv file
    root_path = "/Users/osagieaib/Library/CloudStorage/OneDrive-GodsVisionEnterprise/Documents/Workspace/IT Career/Cedarstone"
    sep = "/"
    read_fdr = "TransformedData"
    dest_path = sep.join([root_path, read_fdr])
    filenames_dict = hu.get_full_path(dest_path, ['csv'])
    # print(filenames_dict)

    sample_fname = "axi_dataset_transformed_daily.csv"
    src_df = pd.read_csv(filenames_dict[sample_fname])
    # print(src_df.info())
    # print(src_df.head())
    # print(src_df.tail())
    df = copy.deepcopy(src_df)
    filenames_dict.pop(sample_fname)
    excl_file = 'axi_dataset_transformed_daily_main.csv'
    daily_return_fpath = filenames_dict.pop(excl_file)
    # print(filenames_dict)
    joined_df = hu.join_to_sample_df(df,
                                     filenames_dict)
    # print(joined_df.info())
    # print(joined_df.head())

    df = copy.deepcopy(joined_df)

    # extract daily return df
    daily_return_df = pd.read_csv(daily_return_fpath)
    df2 = copy.deepcopy(daily_return_df)

    # recast calendar date column
    date_cols = ['calendar_day']
    df = hu.recast_dtypes(df,
                          date_cols=date_cols)
    print(df.info())
    # print(df.head())
    # print(df.tail())

    df2 = hu.recast_dtypes(df2,
                          date_cols=date_cols)
    # print(df2.info())

    # COMPUTE CORE KPIs & RATIOs
    # start and end dates
    col_list = ['calendar_day',
                'date_key',
                'trades_opened']
    cond = (df[col_list[-1]]>0)
    start_date = datetime.datetime.date(df.loc[cond, col_list[:-1]].min()[col_list[0]])
    end_date = datetime.datetime.date(df.loc[cond, col_list[:-1]].max()[col_list[0]])

    # total trades (i.e tickets count)
    closed_trades = df['trades_closed'].sum()
    ongoing_trades = df['trades_ongoing'].sum()
    n_trades = df['trades_opened'].sum()
    lots_traded = df2['traded_lots'].sum()
    pips_gained = df2['pips_gained'].sum()

    # max amount won
    col_list = ['calendar_day',
                'symbol',
                'max_profit',
                'max_loss']
    cond = (df[col_list[2]] == df[col_list[2]].max())
    max_profit = round(df.loc[cond, col_list[2]].values[0], 2)
    max_profit_date = df.loc[cond, col_list[0]].dt.date.values[0]
    max_profit_symbol = df.loc[cond, col_list[1]].values[0].upper()
    # max amount lost
    cond = (df[col_list[-1]] == df[col_list[-1]].min().round(2))
    max_loss = round(df.loc[cond, col_list[-1]].values[0], 2)
    max_loss_date = df.loc[cond, col_list[0]].dt.date.values[0]
    max_loss_symbol = df.loc[cond, col_list[1]].values[0].upper()

    # win = profit yielding trade
    # win rate(%) = wins/total (i.e winning tickets/total trades)
    n_wins = df['trades_won'].sum()
    n_longs = df['longs_closed'].sum()
    n_shorts = df['shorts_closed'].sum()
    longs_won = df['longs_won'].sum()
    shorts_won = df['shorts_won'].sum()

    # win rates
    win_rate = round((n_wins / closed_trades) * 100, 0)
    long_win_rate = round((longs_won / n_longs) * 100, 2)
    short_win_rate = round((shorts_won / n_shorts) * 100, 2)

    # amount won
    amount_won = round(df['amount_won'].sum(), 2)
    long_amount_won = round(df['long_amount_won'].sum(), 2)
    short_amount_won = round(df['short_amount_won'].sum(), 2)

    # percentage won
    long_perc_amount_won = round((long_amount_won / amount_won) * 100, 2)
    short_perc_amount_won = round((short_amount_won / amount_won) * 100, 2)

    # losses
    n_losses = df['trades_lost'].sum()
    longs_lost = df['longs_lost'].sum()
    shorts_lost = df['shorts_lost'].sum()

    # loss rates
    loss_rate = round((n_losses / closed_trades) * 100, 0)
    long_loss_rate = round((longs_lost / n_longs) * 100, 2)
    short_loss_rate = round((shorts_lost / n_shorts) * 100, 2)

    # amount lost
    amount_lost = round(df['amount_lost'].sum(), 2)
    long_amount_lost = round(df['long_amount_lost'].sum(), 2)
    short_amount_lost = round(df['short_amount_lost'].sum(), 2)

    # percentage lost
    long_perc_amount_lost = round((long_amount_lost / amount_lost) * 100, 2)
    short_perc_amount_lost = round((short_amount_lost / amount_lost) * 100, 2)

    # neutrals
    n_neutrals = df['trades_neutral'].sum()
    neutral_rate = round((n_neutrals/closed_trades) * 100, 0)

    # average win vs average loss (avg = sum/count)
    # average win = sum(profit)/number of winning tickets
    pips_won = df['pips_won'].sum()
    avg_win_profit = round(amount_won/n_wins, 2)
    # avg_win_profit = round(total_net_profit / closed_trades, 2)
    avg_win_pips = round(pips_won/n_wins, 2)
    # avg_win_pips = round(total_win_pips / closed_trades, 2)

    # average loss = sum(loss)/number of losing tickets
    pips_lost = df['pips_lost'].sum()
    avg_net_loss = round(amount_lost/n_losses, 2)
    # avg_net_loss = round(total_net_loss / closed_trades, 2)
    avg_loss_pips = round(pips_lost/n_losses, 2)
    # avg_loss_pips = round(total_loss_pips / closed_trades, 2)

    # scalping and overnight trade
    scalping_trades = df2['trade_scalping'].sum()
    overnight_trades = df2['overnight_trade'].sum()
    # won
    scalping_trades_won = df['trade_scalping_won'].sum()
    overnight_trades_won = df['overnight_trade_won'].sum()
    # amount
    scalping_amount_won = df['scalping_amount_won'].sum()
    overnight_amount_won = df['overnight_amount_won'].sum()
    scalping_amount_lost = df['scalping_amount_lost'].sum()
    overnight_amount_lost = df['overnight_amount_lost'].sum()

    # percentage scalping and overnight trade
    perc_scalping_trades = round((scalping_trades/n_trades) * 100, 2)
    perc_overnight_trades = round((overnight_trades/n_trades) * 100, 2)
    perc_scalping_trades_won = round((scalping_trades_won / scalping_trades) * 100, 2)
    perc_overnight_trades_won = round((overnight_trades_won / overnight_trades) * 100, 2)
    # amount
    perc_scalping_amount_won = round((scalping_amount_won / amount_won) * 100, 2)
    perc_overnight_amount_won = round((overnight_amount_won / amount_won) * 100, 2)
    perc_scalping_amount_lost = round((scalping_amount_lost / amount_lost) * 100, 2)
    perc_overnight_amount_lost = round((overnight_amount_lost / amount_lost) * 100, 2)

    # maximum drawdown (optional)

    # sharpe ratio = (average return/stddev) * sqrt(252)
    # average_return = round(df['avg_return'].mean() * 100, 2)
    average_return = round(df2['avg_return'].mean() * 100, 2)
    # std_return = round(df["std_return"].mean() * 100, 2)
    std_return = round(df2["std_return"].mean() * 100, 2)
    std_neg_return = round(df2["std_neg_return"].mean() * 100, 2)
    # sharpe_ratio = round((average_return/std_return) * np.sqrt(252), 2)
    sharpe_ratio = round((average_return / std_return) * np.sqrt(252), 2)

    # sortino ratio = (average return/stddev of negative returns) * sqrt(252)
    sortino_ratio = round((average_return / std_neg_return) * np.sqrt(252), 2)

    # profit factor = total profits/total losses
    profit_factor = abs(round((amount_won/amount_lost), 2))

    # PRESENTATION
    print(f"\nACCCOUNT: {acct_nr}")
    print(f"\nSTART DATE: {start_date}"
          f"\nEND DATE: {end_date}"
          f"\nTOTAL TRADES: {n_trades}"
          f"\nOPEN TRADES: {ongoing_trades}"
          f"\nCLOSED TRADES: {closed_trades}"
          #Lots traded and pips gained
          f"\nLOTS TRADED: {lots_traded}"
          f"\nNET PIPS GAINED: {pips_gained}"
          )

    # WINS
    print(f"\nWIN RATE: {win_rate}% ({n_wins}/{closed_trades})"
          f"\nLONG WIN RATE: {long_win_rate}% ({longs_won}/{n_longs})"
          f"\nSHORT WIN RATE: {short_win_rate}% ({shorts_won}/{n_shorts})"
        
          f"\nAMOUNT WON: £{amount_won}"
          f"\nMAX AMOUNT WON: £{max_profit} ON {max_profit_date} "
          f"DURING {max_profit_symbol} POSITION"
          f"\nLONG AMOUNT WON: £{long_amount_won} ({long_perc_amount_won}%)"
          f"\nSHORT AMOUNT WON: £{short_amount_won} ({short_perc_amount_won}%)")
    print(f"\nAVERAGE WIN: £{avg_win_profit}/{avg_win_pips}pips")

    # LOSSES
    print(f"\nLOSS RATE: {loss_rate}% ({n_losses}/{closed_trades})"
          f"\nLONG LOSS RATE: {long_loss_rate}% ({longs_lost}/{n_longs})"
          f"\nSHORT LOSS RATE: {short_loss_rate}% ({shorts_lost}/{n_longs})"
          
          f"\nAMOUNT LOST: £{abs(amount_lost)}"
          f"\nMAX AMOUNT LOST: £{abs(max_loss)} ON {max_loss_date} "
          f"DURING {max_loss_symbol} POSITION"
          f"\nLONG AMOUNT LOST: £{abs(long_amount_lost)} ({long_perc_amount_lost}%)"
          f"\nSHORT AMOUNT LOST: £{abs(short_amount_lost)} ({short_perc_amount_lost}%)")
    print(f"\nAVERAGE LOSS: £{avg_net_loss}/{avg_loss_pips}pips")

    # SCALPING VS OVERNIGHT
    print(f"\nSCALPING TRADES: {scalping_trades} ({perc_scalping_trades}%)"
          f"\nOVERNIGHT TRADES: {overnight_trades} ({perc_overnight_trades}%)"
          f"\nSCALPING TRADES WON: {scalping_trades_won} ({perc_scalping_trades_won}%)"
          f"\nOVERNIGHT TRADES WON: {overnight_trades_won} ({perc_overnight_trades_won}%)"
          
          f"\nSCALPING AMOUNT WON: £{scalping_amount_won} ({perc_scalping_amount_won}%)"
          f"\nOVERNIGHT AMOUNT WON: £{overnight_amount_won} ({perc_overnight_amount_won}%)"
          f"\nSCALPING AMOUNT LOST: £{scalping_amount_lost} ({perc_scalping_amount_lost}%)"
          f"\nOVERNIGHT AMOUNT LOST: £{overnight_amount_lost} ({perc_overnight_amount_lost}%)"
          )

    # NEUTRALS
    print(f"\nTOTAL NEUTRALS: {n_neutrals}"
          f"\nNEUTRAL RATE (%): {neutral_rate}")

    # RETURNS
    print(f"\nAVERAGE RETURN: £{average_return}"
          # f"\nAVERAGE RETURN: £{average_return2}"
          f"\nSTD DEV OF RETURN: £{std_return}"
          # f"\nSTD DEV OF RETURN: £{std_return2}"
          f"\nSTD DEV OF -VE RETURN: £{std_neg_return}"
          # f"\nSHARPE RATIO: {sharpe_ratio2}"
          f"\nSHARPE RATIO: {sharpe_ratio}"
          f"\nSORTINO RATIO: {sortino_ratio}"
          f"\nPROFIT FACTOR (%): {profit_factor}")