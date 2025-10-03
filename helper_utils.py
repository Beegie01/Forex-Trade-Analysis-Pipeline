import io
import copy
import pandas as pd
import tkinter as tk
from tkinter import filedialog as fd
import os
import datetime
from sqlalchemy import text
import sqlalchemy as db

class HelperUtils:

    @staticmethod
    def refresh_pgsql_mview(db_conn: 'database connection',
                            refresh_schema: str,
                            refresh_mview_name: str):
        """refresh materialized view"""

        db_conn.execute(text(f"REFRESH MATERIALIZED VIEW {refresh_schema}.{refresh_mview_name};"))
        print("Materialized view refreshed successfully.")

    @staticmethod
    def dbase_conn_sqlalchemy(dbase_name: str, dbase_password: str, dbase_driver: str = 'postgresql',
                              dbase_username: str = 'postgres', dbase_host: str = 'localhost', dbase_port: int = 5432):
        """connect to a database session using sqlalchemy
        output: dict(engine obj, connection obj)"""

        # create instance of database session for the given database
        db_engine = db.create_engine(
            url=f'{dbase_driver}://{dbase_username}:{dbase_password}@{dbase_host}:{dbase_port}/{dbase_name}')
        print('ENGINE CREATED')

        # connect to database instance
        db_conn = db_engine.connect()
        print('CONNECTION CREATED')

        return {'engine': db_engine, 'connection': db_conn}

    @staticmethod
    def sqlalchem_select_query(table_name: str, schema_name: str, dbase_engine: "sqlalchemy engine instance",
                               dbase_conn: "database connection instance"):
        """retrieve data from database table
         output: dataframe object"""

        # create metadata object
        db_metadata = db.MetaData()

        # db_table = db.Table('support_plan_log', db_metadata, schema='supported housing', autoload_with=db_engine)
        db_table = db.Table(table_name, db_metadata, schema=schema_name, autoload_with=dbase_engine)
        print('TABLE OBJECT CREATED')

        # collect table header
        header = db_table.columns.keys()

        # create select query object for table
        query = db.select(db_table)
        # execute query
        output = dbase_conn.execute(query)
        # store query result
        data = output.fetchall()
        # parse query result as dataframe
        df = pd.DataFrame(data=data, columns=header)
        print('\nQUERY OUTPUT IS AVAILABLE')

        return df

    @staticmethod
    def concat_column_values(df: 'pd.DataFrame',
                             column_names: 'list of columns' = list(),
                             separator='-;-',
                             delta_id='delta_id'):
        """concatenate multiple columns into a single column
        df: dataframe containing parent dataset
        column_names: list of columns whose values are to be selected for concatenation
        separator: separating character
        delta_id: name of new column containing concatenated values

        Output: dataframe including concatenated column"""

        col_size = len(column_names)
        n_records = df.shape[0]
        if n_records > 0:
            if col_size < 1:
                # print(df.astype(str).agg(func=f'{separator}'.join, axis='columns'))
                df[delta_id] = df.astype(str).agg(func=f'{separator}'.join, axis='columns')
            else:
                # print(df[column_names].astype(str).agg(func=f'{separator}'.join, axis='columns'))
                df[delta_id] = df[column_names].astype(str).agg(func=f'{separator}'.join, axis='columns')
        else:
            df[delta_id] = ''
        print(df.info())
        return df

    @staticmethod
    def append_to_sample_df(sample_df: "pandas dataframe",
                            file_path_dict: dict,
                            get_data: str = "ft",
                            use_func="cleaning") -> 'pandas dataframe':
        """append data extracted from all remaining files in the source folder
        to data extracted from model file (aka sample file)
        returns dataframe containing the complete dataset from files in source folder"""

        df_list = [sample_df]
        eval_func = ""
        # for excel files
        for f, fpath in file_path_dict.items():
            file_name, file_ext = f.split('.')
            # print(file_comp)
            print(f'\nReading {file_name}')
            eval_func = f"HelperUtils.{use_func}_steps(fpath)"
            acct_df_dict = eval(eval_func)
            df = acct_df_dict['fact_df']
            if get_data == 'dt':
                df = acct_df_dict['dim_account']

            df_list.append(df)

        cache_df = pd.concat(df_list)

        return cache_df

    @staticmethod
    def join_to_sample_df(sample_df: "pandas dataframe",
                            file_path_dict: dict) -> 'pandas dataframe':
        """join data extracted from all remaining files in the source folder
        to data extracted from model file (aka sample file)
        returns dataframe containing the conjoined columns from files in source folder"""

        files_dd = list(file_path_dict.items())
        cache_df = pd.DataFrame()
        # for csv files
        for i in range(len(files_dd)):
            f, fpath = files_dd[i]
            file_name, file_ext = f.split('.')
            # print(file_comp)
            print(f'\nReading {file_name}')
            df = pd.read_csv(fpath)
            # print(df.info())
            if i==0:  # first iteration
                cache_df = pd.merge(left=sample_df,
                              right=df,
                              left_on=['calendar_day',
                                       'date_key',
                                       'day_name',
                                       'symbol'],
                              right_on=['calendar_day',
                                        'date_key',
                                        'day_name',
                                        'symbol'],
                              how='left')
            else:
                cache_df = pd.merge(left=cache_df,
                              right=df,
                              left_on=['calendar_day',
                                       'date_key',
                                       'day_name',
                                       'symbol'],
                              right_on=['calendar_day',
                                        'date_key',
                                        'day_name',
                                        'symbol'],
                              how='left')

        return cache_df

    @staticmethod
    def run_delta_load_to_db(new_data: 'pd.DataFrame',
                             old_data: "pd.DataFrame",
                             delta_col_name: str,
                             database_table_name: str,
                             db_engine: 'sqlalchemy create engine obj',
                             db_schema: str,
                             load_to_db=True):
        """run delta load logic into a connected database
        :parameter
        new_data: dataframe containing fresh data
        old_data: dataframe containing data in the existing dbms table
        delta_col_name:identify the differences in values of the delta columns in both datasets
        load_to_db - if true, load data in the delta_load dataframe into the target dbms table

        Logic:
        - get delta column data of both new and existing records
        - only select the records not present in the existing database table
        """

        # get fresh data's unique values in the delta column
        new_set = set(new_data[delta_col_name])
        print(f'\n{len(new_set)} new records found')
        # print(new_set)

        # get existing data's unique values in the delta column
        old_set = set(old_data[delta_col_name])
        print(f'\n{len(old_set)} existing db records found')
        # print(old_set)

        # get values from new data not present in the existing db data
        delta_id = new_set.difference(old_set)
        # select only fresh data
        # use_colnames = list(old_data.columns)
        delta_load = new_data.loc[new_data[delta_col_name].isin(delta_id)]

        # drop the delta_id before loading to the database
        delta_load = delta_load.drop(columns=delta_col_name)
        print('\nDelta Load:')
        print(delta_load.info())

        # load only fresh data into target database table
        if load_to_db:
            delta_load.to_sql(name=database_table_name,
                              con=db_engine,
                              schema=db_schema,
                              if_exists='append',
                              index=False)
            print("Delta load done!")

        return delta_load

    @staticmethod
    def generate_calendar(start_date: 'date',
                          end_date: 'date' = datetime.date.today(),
                          date_colname: str = 'calendar_day',
                          include_date_features=False) -> 'dataframe':
        """generate calendar dataframe starting from the start_date
        up till end_date
        output:
        if include_date_features is False:
        ['calendar_day', 'date_key']
        if include_date_features is True:
        ['calendar_day', 'date_key', 'year', 'is_leap_year', 'is_year_start',
        'is_year_end', 'quarter', 'quarter_name', 'is_quarter_start',
        'is_quarter_end', 'month', 'month_name', 'month_days', 'is_month_start',
        'is_month_end', 'week_of_year', 'day', 'day_name', 'day_of_year', 'day_of_week']"""

        date_df = pd.DataFrame({date_colname: pd.date_range(start=start_date, end=end_date)}).astype('datetime64[ns]')
        date_df['date_key'] = date_df['calendar_day'].astype('str').str.replace('-', '').astype('Int64')
        if include_date_features:
            date_df['year'] = date_df[date_colname].dt.year
            date_df['is_leap_year'] = date_df[date_colname].dt.is_leap_year
            date_df['is_year_start'] = date_df[date_colname].dt.is_year_start
            date_df['is_year_end'] = date_df[date_colname].dt.is_year_end
            date_df['quarter'] = date_df[date_colname].dt.quarter
            date_df['quarter_name'] = date_df['quarter'].apply(lambda x: f"q{x}")
            date_df['is_quarter_start'] = date_df[date_colname].dt.is_quarter_start
            date_df['is_quarter_end'] = date_df[date_colname].dt.is_quarter_end
            date_df['month'] = date_df[date_colname].dt.month
            date_df['month_name'] = date_df[date_colname].dt.month_name()
            date_df['month_days'] = date_df[date_colname].dt.daysinmonth
            date_df['is_month_start'] = date_df[date_colname].dt.is_month_start
            date_df['is_month_end'] = date_df[date_colname].dt.is_month_end
            date_df['week_of_year'] = date_df[date_colname].dt.isocalendar().week
            date_df['day'] = date_df[date_colname].dt.day
            date_df['day_name'] = date_df[date_colname].dt.day_name()
            date_df['day_of_year'] = date_df[date_colname].dt.dayofyear
            date_df['day_of_week'] = date_df[date_colname].dt.dayofweek

        return date_df

    @staticmethod
    def switch_col_values(ser: 'pandas series', mapper: 'dictionary object'):
        """switch an old value with a new one as mapped in the given dictionary
        output: pandas series"""

        new_ser = ser.apply(lambda x: mapper[x])
        return new_ser

    @staticmethod
    def configure_display_settings():
        """configure display settings for pandas output"""

        pd.options.display.width = None
        pd.options.display.max_columns = None
        pd.set_option('display.max_rows', 1000)
        pd.set_option('display.max_columns', 300)

    @staticmethod
    def get_full_path(folder_path: str, specify_ftype: 'str or list' = None) -> dict:
        '''get dictionary of "filename"":"fullpath" for all the
        files contained within the given folder
        folder_path: full path of folder in which to search
        specify_ftype: only retrieve the specified file type'''

        filenames_list = os.listdir(folder_path)
        file_dict = dict()
        sep = "/"
        if specify_ftype is not None and isinstance(specify_ftype, str):
            for i in range(len(filenames_list)):
                if f'.{specify_ftype}' in filenames_list[i]:
                    fpath = sep.join([folder_path, filenames_list[i]])
                    file_dict[filenames_list[i]] = fpath
        elif specify_ftype is not None and isinstance(specify_ftype, list):
            for f in range(len(specify_ftype)):
                for i in range(len(filenames_list)):
                    if f'.{specify_ftype[f]}' in filenames_list[i]:
                        fpath = sep.join([folder_path, filenames_list[i]])
                        file_dict[filenames_list[i]] = fpath
        else:
            for i in range(len(filenames_list)):
                fpath = sep.join([folder_path, filenames_list[i]])
                file_dict[filenames_list[i]] = fpath
        return file_dict

    @staticmethod
    def extract_htm_file(url_path: str) -> 'list of pandas dataframe':
        """read data from an htm file
        output a list of pandas data frame"""

        # extract data as string directly from htm file
        with open(file=url_path) as f:
            read_data = f.read()
        # print(file_data)

        file_data = io.StringIO(read_data)
        # print(type(file_data))
        # print(type(file_data.read()))
        # print(file_data.read())
        # directly convert into a list of dataframe string containing html data (including tags)
        df_list = pd.read_html(file_data)
        return df_list

    @staticmethod
    def select_files_dialog(starting_loc: str,
                            prompt_text: str = "FILE(S) SELECTION", ):
        """select as many files as you like to record their absolute file path"""

        root = tk.Tk()
        selected_file_list = fd.askopenfilenames(parent=root,
                                                 title=prompt_text,
                                                 initialdir=starting_loc)
        return selected_file_list

    @staticmethod
    def recast_dtypes(df: 'pandas dataframe', show_progress=True, date_cols: 'list of date column names' = list(),
                      flt_cols: 'list of date column names' = list(),
                      int_cols: 'list of integer column names' = list(),
                      str_cols: 'list of alphanumeric columns' = list()):
        """enforce column data type in the given dataframe
        output: pandas dataframe"""

        if len(date_cols):
            for i in range(len(date_cols)):
                date_dtype = df[date_cols[i]].astype('datetime64[ns]')
                df[date_cols[i]] = date_dtype
                if show_progress:
                    print(f"{date_cols[i]}'s DATATYPE CHANGED!")
            if show_progress:
                print('date columns assigned')

        if len(flt_cols):
            for i in range(len(flt_cols)):
                flt_dtype = pd.to_numeric(df[flt_cols[i]], errors='coerce').astype('float64')
                df[flt_cols[i]] = flt_dtype
                if show_progress:
                    print(f"{flt_cols[i]}'s DATATYPE CHANGED!")
            if show_progress:
                print('decimal columns assigned')

        if len(int_cols):
            for i in range(len(int_cols)):
                int_dtype = pd.to_numeric(df[int_cols[i]], errors='coerce')  # Converts invalid entries to NaN
                int_dtype = int_dtype.astype('Int64')
                df[int_cols[i]] = int_dtype
                if show_progress:
                    print(f"{int_cols[i]}'s DATATYPE CHANGED!")
            if show_progress:
                print('integer columns assigned')

        if len(str_cols):
            for i in range(len(str_cols)):
                str_dtype = df[str_cols[i]].astype('str')
                df[str_cols[i]] = str_dtype
                if show_progress:
                    print(f"{str_cols[i]}'s DATATYPE CHANGED!")
            if show_progress:
                print('string columns assigned')

        # print(df.info())
        if show_progress:
            print('\nCOLUMN DATA TYPE RECAST COMPLETE\n')

        return df

    @staticmethod
    def cleaning_steps(file_loc) -> 'pandas dataframe':
        """clean file based on file structure"""

        # extract source data into dataframe
        src_df = pd.read_excel(file_loc)
        # print(src_df.info())
        # print(src_df.head(5))
        df = copy.deepcopy(src_df)
        # print(df.head())

        # get account id from dataset
        col_list = ['AxiCorp Financial Services Pty Ltd']
        cond = df[col_list[0]].astype(str).str.contains("Account:")
        acct_nr = int(df.loc[cond, col_list[0]].astype(str).str.split(": ").str[-1].str.strip().iloc[0])

        # create account number column
        df["Account Number"] = acct_nr

        # drop first 4 rows
        main_records = df.iloc[4:]

        # rename column headers appropriately
        col_map = {"AxiCorp Financial Services Pty Ltd": "Ticket",
                   "Unnamed: 1": "Open DateTime",
                   "Unnamed: 8": "Close DateTime",
                   "Unnamed: 2": "Type",
                   "Unnamed: 4": "Symbol",
                   "Unnamed: 3": "Size",
                   "Unnamed: 5": "Open Price",
                   "Unnamed: 9": "Close Price",
                   "Unnamed: 10": "Commission",
                   "Unnamed: 12": "Swap",
                   "Unnamed: 13": "Profit",
                   "Unnamed: 6": "SL",
                   "Unnamed: 7": "TP"}
        main_records = main_records.rename(columns=col_map)
        # print(main_records.info())
        # print(main_records.head(5))
        df = copy.deepcopy(main_records)

        # drop irrelevant columns
        col_list = ["Account Number",
                    "Ticket",
                    "Open DateTime",
                    "Close DateTime",
                    "Type",
                    "Symbol",
                    "Size",
                    "Open Price",
                    "Close Price",
                    "Commission",
                    "Swap",
                    "Profit",
                    "SL",
                    "TP"]
        keep_columns = df[col_list]
        df = copy.deepcopy(keep_columns)

        # drop duplicate ticket numbers
        drop_duplicates = df.drop_duplicates(subset=['Account Number', 'Ticket'], keep='last')
        df = copy.deepcopy(drop_duplicates)

        # keep only buy, sell, or balance records
        cond = df['Type'].isin(["buy", "sell", "balance"])
        buy_sell_trans = df[cond].reset_index(drop=True)
        df = copy.deepcopy(buy_sell_trans)
        # print(df.info())

        # engineer separate date and time columns from datetime column
        added_open_date = df['Open DateTime'].str.replace('.', '-').astype('datetime64[ns]').dt.date
        df['Open Date'] = added_open_date
        added_open_date_key = df['Open DateTime'].str.split(' ').str[0].str.replace('.', '')
        df['Open Date Key'] = added_open_date_key
        added_open_time = df['Open DateTime'].str.replace('.', '-').astype('datetime64[ns]').dt.time
        df['Open Time'] = added_open_time
        added_close_date = df['Close DateTime'].str.replace('.', '-').astype('datetime64[ns]').dt.date
        df['Close Date'] = added_close_date
        added_close_date_key = df['Close DateTime'].str.split(' ').str[0].str.replace('.', '')
        df['Close Date Key'] = added_close_date_key
        added_close_time = df['Close DateTime'].str.replace('.', '-').astype('datetime64[ns]').dt.time
        df['Close Time'] = added_close_time

        # drop irrelevant columns
        col_list = ["Account Number",
                    "Ticket",
                    "Open DateTime",
                    "Close DateTime",
                    "Open Date",
                    "Open Date Key",
                    "Close Date",
                    "Close Date Key",
                    "Open Time",
                    "Close Time",
                    "Type",
                    "Symbol",
                    "Size",
                    "Open Price",
                    "Close Price",
                    "Commission",
                    "Swap",
                    "Profit",
                    "SL",
                    "TP"]
        keep_columns = df[col_list]
        df = copy.deepcopy(keep_columns)

        # recast columns to appropriate data types
        intg_cols = ["Account Number",
                     "Ticket",
                     "Open Date Key",
                     "Close Date Key"]
        flg_cols = ["Size",
                    "Open Price",
                    "Close Price",
                    "Commission",
                    "Swap",
                    "Profit",
                    "SL",
                    "TP"]
        date_cols = ["Open DateTime",
                     "Close DateTime",
                     "Open Date",
                     "Close Date"]
        recast_dtypes = HelperUtils.recast_dtypes(df,
                                                  show_progress=False,
                                                  date_cols=date_cols,
                                                  int_cols=intg_cols,
                                                  flt_cols=flg_cols)
        df = copy.deepcopy(recast_dtypes)

        # sort by date in ascending order
        sort_cols = ['Open DateTime',
                     "Close DateTime",
                     "Symbol",
                     "Type"]
        dates_sorted = df.sort_values(by=sort_cols,
                                      ascending=[True, True, True, True])
        df = copy.deepcopy(dates_sorted)

        # engineer trade duration column
        added_trade_duration = ((df['Close DateTime'] - df['Open DateTime']).dt.total_seconds() / 60).round().astype(
            'Int64')
        df["Trade Duration (minutes)"] = added_trade_duration

        # engineer pips gained
        added_pips_gained = df.apply(
            func=lambda col: (col['Close Price'] - col['Open Price']) * 10000 if col['Type'] == "buy" else (col[
                                                                                                                'Open Price'] -
                                                                                                            col[
                                                                                                                'Close Price']) * 10000,
            axis='columns')
        df["Pips Gained"] = added_pips_gained

        # engineer net profit
        added_net_profit = df.apply(lambda col: col['Profit'] - col['Commission'] - col['Swap'],
                                    axis='columns')
        df['Net Profit'] = added_net_profit

        # engineer cumulative profit column
        added_cumulative_profit_col = df['Net Profit'].cumsum()
        df['Cumulative Profit'] = added_cumulative_profit_col

        # engineer balance column
        added_balance_col = df.apply(
            lambda col: col['Profit'] - pd.Series(col['Commission']).fillna(0) - pd.Series(col['Swap']).fillna(0),
            axis='columns').cumsum()
        df['Balance'] = added_balance_col

        # engineer initial balance column
        added_init_balance_col = df.apply(
            lambda col: col['Balance'] - pd.Series(col['Net Profit']).fillna(0),
            axis='columns')
        df['Initial Balance'] = added_init_balance_col

        # keep only buy/sell records
        cond = df['Type'].isin(["buy", "sell"])
        buy_sell_trans = df[cond].reset_index(drop=True)
        df = copy.deepcopy(buy_sell_trans)

        # engineer return column
        # return = (net_profit/initial_balance) * 100
        added_return_col = df.apply(lambda col: round((col['Net Profit'] / col['Initial Balance']), 4),
                                    axis='columns')
        df['Return'] = added_return_col

        # engineer closed trade indicator (for total trades calculation)
        trade_closed = df.apply(
            lambda col: 1 if (col['Open Date'] is not pd.NaT) and (col['Close Date'] is not pd.NaT) else 0,
            axis='columns')
        df['Trade Closed'] = trade_closed

        # engineer win indicator (for win rate calculations on closed trades)
        # 9999 is placeholder for open trades
        trade_won = df.apply(lambda col: 1 if (col["Trade Closed"] == 1) and (col["Net Profit"] > 0) else -1 if (col[
                                                                                                                     "Trade Closed"] == 1) and (
                                                                                                                        col[
                                                                                                                            "Net Profit"] < 0) else 0 if (
                                                                                                                                                                 col[
                                                                                                                                                                     "Trade Closed"] == 1) and (
                                                                                                                                                                 col[
                                                                                                                                                                     "Net Profit"] == 0) else 9999,
                             axis='columns')
        df['Trade Won'] = trade_won

        # engineer stop loss indicator
        sl_triggered = df.apply(lambda col: 1 if (col["SL"] == col["Close Price"]) else 0,
                                axis='columns')
        df['SL Triggered'] = sl_triggered

        # engineer take profit indicator
        tp_triggered = df.apply(lambda col: 1 if (col["TP"] == col["Close Price"]) else 0,
                                axis='columns')
        df['TP Triggered'] = tp_triggered
        # print(df['Trade Won'].value_counts())
        # print(df.loc[df["Trade Closed"] == 0])

        # RISK FLAGGING
        # engineer trade scalping indicator
        trade_scalping = df.apply(
            lambda col: 1 if ((col['Trade Closed'] == 1) and (col['Trade Duration (minutes)'] < 2)) else 0,
            axis='columns')
        df['Trade Scalping'] = trade_scalping

        # engineer high drawdown indicator
        # high drawdown = Profit < -50% of average loss
        col_list = ["Trade Won", "Profit"]
        cond = (df[col_list[0]] == -1)
        avg_loss = df.loc[cond, col_list[-1]].mean().round(2)
        thresh_trigger = avg_loss * 0.5
        high_drawdown = df.apply(lambda col: 1 if col['Profit'] < thresh_trigger else 0,
                                 axis='columns')
        df['High Drawdown'] = high_drawdown

        # engineer losing streak
        # losing streak = 3 consecutive losses
        is_loss = (df['Trade Won'] == -1).astype('Int8')
        df['3 Consecutive Losses'] = ((is_loss & is_loss.shift(1) & is_loss.shift(2)).fillna(0))

        # engineer overnight trades
        # overnight trade = trade duration > 8 hours
        overnight_trade = df.apply(lambda col: 1 if (
                (col['Trade Closed'] == 1) and (round(col['Trade Duration (minutes)'] / 60, 0) > 8)) else 0,
                                   axis='columns')
        df['Overnight Trade'] = overnight_trade

        # replace space character with underscore
        # df.columns = map(lambda col_name: '_'.join(col_name.split()), list(df.columns))

        return df

    @staticmethod
    def prep_steps(file_loc) -> 'pandas dataframe':
        """clean file based on file structure"""

        # extract source data into dataframe
        src_df = pd.read_excel(file_loc)
        # print(src_df.info())
        # print(src_df.head(5))
        df = copy.deepcopy(src_df)
        # print(df.head())
        acct_details = {}

        col_list = ['AxiCorp Financial Services Pty Ltd']
        cond = df[col_list[0]].astype(str).str.contains("Account:")
        acct_details['acct_nr'] = [int(df.loc[cond, col_list[0]].astype(str).str.split(": ").str[-1].str.strip().iloc[0])]
        # print(acct_details)

        col_list = ['Unnamed: 2']
        cond = df[col_list[0]].astype(str).str.contains("Name:")
        acct_details['acct_name'] = [df.loc[cond, col_list[0]].astype(str).str.split(": ").str[-1].str.strip().iloc[0]]
        # print(acct_name)

        col_list = ['Unnamed: 7']
        cond = df[col_list[0]].astype(str).str.contains("Currency:")
        acct_details['acct_currency'] = [df.loc[cond, col_list[0]].astype(str).str.split(": ").str[-1].str.strip().iloc[0]]
        # print(acct_details)

        # create account number column
        df["Account Number"] = acct_details['acct_nr'][0]

        # drop first 4 rows
        main_records = df.iloc[4:]

        # rename column headers appropriately
        col_map = {"AxiCorp Financial Services Pty Ltd": "Ticket",
                   "Unnamed: 1": "Open DateTime",
                   "Unnamed: 8": "Close DateTime",
                   "Unnamed: 2": "Type",
                   "Unnamed: 4": "Symbol",
                   "Unnamed: 3": "Size",
                   "Unnamed: 5": "Open Price",
                   "Unnamed: 9": "Close Price",
                   "Unnamed: 10": "Commission",
                   "Unnamed: 12": "Swap",
                   "Unnamed: 13": "Profit",
                   "Unnamed: 6": "SL",
                   "Unnamed: 7": "TP"}
        main_records = main_records.rename(columns=col_map)
        # print(main_records.info())
        # print(main_records.head(5))
        df = copy.deepcopy(main_records)

        # drop irrelevant columns
        col_list = ["Account Number",
                    "Ticket",
                    "Open DateTime",
                    "Close DateTime",
                    "Type",
                    "Symbol",
                    "Size",
                    "Open Price",
                    "Close Price",
                    "Commission",
                    "Swap",
                    "Profit",
                    "SL",
                    "TP"]
        keep_columns = df[col_list]
        df = copy.deepcopy(keep_columns)

        # drop duplicate ticket numbers
        drop_duplicates = df.drop_duplicates(subset=['Account Number', 'Ticket'],
                                             keep='last')
        df = copy.deepcopy(drop_duplicates)

        # keep only buy, sell, or balance records
        cond = df['Type'].isin(["buy", "sell", "balance"])
        buy_sell_trans = df[cond].reset_index(drop=True)
        df = copy.deepcopy(buy_sell_trans)
        # print(df.info())

        # engineer separate date and time columns from datetime column
        added_open_date_key = df['Open DateTime'].str.split(' ').str[0].str.replace('.', '')
        df['Open Date Key'] = added_open_date_key
        added_close_date_key = df['Close DateTime'].str.split(' ').str[0].str.replace('.', '')
        df['Close Date Key'] = added_close_date_key

        # drop irrelevant columns
        col_list = ["Account Number",
                    "Ticket",
                    "Open DateTime",
                    "Close DateTime",
                    "Open Date Key",
                    "Close Date Key",
                    "Type",
                    "Symbol",
                    "Size",
                    "Open Price",
                    "Close Price",
                    "Commission",
                    "Swap",
                    "Profit",
                    "SL",
                    "TP"]
        keep_columns = df[col_list]
        df = copy.deepcopy(keep_columns)

        # recast columns to appropriate data types
        intg_cols = ["Account Number",
                     "Ticket",
                     "Open Date Key",
                     "Close Date Key"]
        flg_cols = ["Size",
                    "Open Price",
                    "Close Price",
                    "Commission",
                    "Swap",
                    "Profit",
                    "SL",
                    "TP"]
        date_cols = ["Open DateTime",
                     "Close DateTime"]
        recast_dtypes = HelperUtils.recast_dtypes(df,
                                         show_progress=False,
                                         date_cols=date_cols,
                                         int_cols=intg_cols,
                                         flt_cols=flg_cols)
        df = copy.deepcopy(recast_dtypes)

        # sort by date in ascending order
        sort_cols = ['Open DateTime',
                     "Close DateTime",
                     "Symbol",
                     "Type"]
        dates_sorted = df.sort_values(by=sort_cols,
                                      ascending=[True, True, True, True])
        df = copy.deepcopy(dates_sorted)

        # replace space character with underscore
        df.columns = map(lambda col_name: '_'.join(col_name.split()), list(df.columns))
        # print(df.info())

        dim_acct = pd.DataFrame(acct_details)

        out_dict = {'dim_account': dim_acct,
                    'fact_df': df}
        return out_dict