import copy
import pandas as pd
import numpy as np
from helper_utils import HelperUtils as hu
from pg_settings import pg_var as pvr


def run_app(run_dialog=True):

    # configure display settings for pandas output
    hu.configure_display_settings()

    # DATA EXTRACTION
    root_path = "/Users/osagieaib/Library/CloudStorage/OneDrive-GodsVisionEnterprise/Documents/Workspace/IT Career/Cedarstone"
    read_fdr = "CleansedData"
    sep = "/"
    rfile_name = "axi_dataset_cleaned_dw_dim.csv"
    dest_path = sep.join([root_path, read_fdr, rfile_name])

    # extract cleansed dataset
    src_df = pd.read_csv(dest_path)
    # print(src_df.info())
    df = copy.deepcopy(src_df)
    col_list = list(df.columns)

    added_delta_col = hu.concat_column_values(df=df,
                                              column_names=col_list,
                                              delta_id="unique_identifier")
    # drop rows having duplicate unique_identifiers
    keep_unique_records = added_delta_col.drop_duplicates(subset=['unique_identifier'])
    unique_df = keep_unique_records[col_list]
    df = copy.deepcopy(unique_df)
    # print(df.info())
    # print(df.head(5))

    col_map = {
        "acct_nr":"account_number",
        "acct_name":"account_name",
        "acct_currency":"account_currency"
    }
    df = df.rename(col_map,
                   axis='columns')

    # recast columns to appropriate datatypes
    intg_cols = ['account_number']
    df = hu.recast_dtypes(df,
                          int_cols=intg_cols)

    # engineer a combo variable for delta columns (ie unique row identifiers)
    col_order = list(df.columns)
    delta_id = 'delta_id'
    df = hu.concat_column_values(df, column_names=col_order, delta_id=delta_id)

    # sort source data in specific order
    sort_cols = ["account_number"]
    sorted_df = df.sort_values(by=sort_cols)
    final_df = copy.deepcopy(sorted_df)
    # print(final_df.info())
    # print(df.head(5))

    # create instance of database session for the database
    # db_user = pvr['db_user']
    db_pwd = pvr['db_pwd']
    db_name = pvr['db_name']
    # db_host = pvr['db_host']
    db_port = pvr['db_port']
    dbase_cred = hu.dbase_conn_sqlalchemy(dbase_name=db_name,
                                          dbase_password=db_pwd,
                                          dbase_port=db_port)
    dbase_engine = dbase_cred['engine']
    dbase_conn = dbase_cred['connection']

    # get data from database table
    db_table_name = 'dim_account'
    db_schema = 'staging'
    db_table = hu.sqlalchem_select_query(table_name=db_table_name,
                                         schema_name=db_schema,
                                         dbase_engine=dbase_engine,
                                         dbase_conn=dbase_conn)

    intg_cols = ['account_number']
    # replace Python NoneType with pandas NaN
    db_table = db_table.fillna(value=np.nan)

    db_table = hu.recast_dtypes(db_table,
                                int_cols=intg_cols)
    # print(db_table.info())

    # engineer a combo variable for delta columns (ie unique row identifiers)
    db_table = hu.concat_column_values(db_table,
                                       column_names=col_order,
                                       delta_id=delta_id)
    # sort target data in specific order
    sort_cols = ["account_number"]
    db_table = db_table.sort_values(by=sort_cols)

    # print(db_table.info())
    # print(db_table.head(5))

    # load into target database table
    print('\nLOADING FRESH RECORDS INTO TARGET')
    delta_load = hu.run_delta_load_to_db(new_data=final_df,
                                         old_data=db_table,
                                         delta_col_name=delta_id,
                                         database_table_name=db_table_name,
                                         db_engine=dbase_engine,
                                         db_schema=db_schema)

    # delete old record from database
    print('\nCHECKING FOR REDUNDANT RECORD IN THE DATABASE')
    db_rec_del = hu.run_delta_load_to_db(new_data=db_table,
                                         old_data=final_df,
                                         delta_col_name=delta_id,
                                         database_table_name=db_table_name,
                                         db_engine=dbase_engine,
                                         db_schema=db_schema,
                                         load_to_db=False)
    print(f'\nDelete {db_rec_del.shape[0]} old record form database')
    # print(db_rec_del)
    print(final_df['account_number'].value_counts())

    print("\nLOAD JOB COMPLETE!\n")
