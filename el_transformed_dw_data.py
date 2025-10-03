import copy
import pandas as pd
import numpy as np
from helper_utils import HelperUtils as hu
from pg_settings import pg_var as pvr


def run_app():

    # configure display settings for pandas output
    hu.configure_display_settings()

    # DATA EXTRACTION
    root_dir = "/Users/osagieaib/Library/CloudStorage/OneDrive-GodsVisionEnterprise/Documents/Workspace/IT Career/Cedarstone"
    sep = "/"

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

    # refresh materialized view
    refresh_schema = "core"
    refresh_view = "trade_hist_info"
    hu.refresh_pgsql_mview(db_conn=dbase_conn,
                            refresh_schema=refresh_schema,
                            refresh_mview_name=refresh_view)

    # load transformed view onto dataframe for csv export
    db_schema = 'public'
    db_transformation_name = 'trade_history'
    trade_history = hu.sqlalchem_select_query(table_name=db_transformation_name,
                                              schema_name=db_schema,
                                              dbase_engine=dbase_engine,
                                              dbase_conn=dbase_conn)
    print(trade_history.info())

    db_transformation_name = 'dim_date'
    dim_date = hu.sqlalchem_select_query(table_name=db_transformation_name,
                                              schema_name=db_schema,
                                              dbase_engine=dbase_engine,
                                              dbase_conn=dbase_conn)
    print(dim_date.info())

    db_transformation_name = 'dim_clock'
    dim_clock = hu.sqlalchem_select_query(table_name=db_transformation_name,
                                              schema_name=db_schema,
                                              dbase_engine=dbase_engine,
                                              dbase_conn=dbase_conn)
    print(dim_clock.info())

    db_transformation_name = 'dim_account_info'
    dim_account_info = hu.sqlalchem_select_query(table_name=db_transformation_name,
                                              schema_name=db_schema,
                                              dbase_engine=dbase_engine,
                                              dbase_conn=dbase_conn)
    print(dim_account_info.info())

    # close engine
    dbase_conn.close()
    dbase_engine.dispose()

    # export transformation to csv file
    read_fdr = "TransformedData"
    wfile_name = "fact_trade_history.csv"
    wfile_path = sep.join([root_dir, read_fdr, wfile_name])
    trade_history.to_csv(wfile_path, index=False, encoding='utf8')

    wfile_name = "dim_account_info.csv"
    wfile_path = sep.join([root_dir, read_fdr, wfile_name])
    dim_account_info.to_csv(wfile_path, index=False, encoding='utf8')

    wfile_name = "dim_clock.csv"
    wfile_path = sep.join([root_dir, read_fdr, wfile_name])
    dim_clock.to_csv(wfile_path, index=False, encoding='utf8')

    wfile_name = "dim_date.csv"
    wfile_path = sep.join([root_dir, read_fdr, wfile_name])
    dim_date.to_csv(wfile_path, index=False, encoding='utf8')

    print(trade_history['account_nr'].value_counts())

    print("\nLOAD JOB COMPLETE!\n")
