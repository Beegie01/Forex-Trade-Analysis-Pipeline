import copy
import sys
import pandas as pd
import os
from helper_utils import HelperUtils as hu

# configure display settings
pd.options.display.width = None
pd.options.display.max_columns = None
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 300)

def run_app():

    # load sample dataset
    # root_dir = "https://godsvisionenterprise24-my.sharepoint.com/personal/o_aibangbee_godsvisionenterprise24_onmicrosoft_com/Documents/Documents/Workspace/IT Career/Cedarstone"
    root_dir = "/Users/osagieaib/Library/CloudStorage/OneDrive-GodsVisionEnterprise/Documents/Workspace/IT Career"
    read_fdr = "Cedarstone"
    sample_fname = "Statement.htm"
    sep = '/'
    url_path = sep.join([root_dir, read_fdr, sample_fname])
    # url_path ="/Users/osagieaib/Library/CloudStorage/OneDrive-GodsVisionEnterprise/Documents/Workspace/IT Career/Cedarstone/Statement.htm"
    # driver = webdriver.Safari()
    # driver.implicitly_wait(30)
    # driver.get(url_path)

    # df = pd.read_html(io=driver.find_element("Closed Transactions").get_attribute("outerHTML")[0], header=0, encoding='utf8')
    # print(df.head(10))

    df_list = hu.extract_htm_file(url_path)
    df = df_list[0]
    headers_list = list(df.loc[2])
    # print(headers_list)

    drop_top_3_rows = df.iloc[3:].reset_index(drop=True)
    # print(drop_top_3_rows)

    df = copy.deepcopy(drop_top_3_rows)

    df.columns = headers_list
    # print(df.info())

    # cond = df['Type'].isin(["buy", "sell"])
    # drop_invalid_transactions = df.loc[cond].reset_index(drop=True)
    # print(drop_invalid_transactions)
    #
    # df = copy.deepcopy(drop_invalid_transactions)
    print(df.info())

    # create folder if it does not exist
    read_fdr = "Cedarstone/CleansedData"
    dest_path = sep.join([root_dir, read_fdr])
    os.makedirs(dest_path, exist_ok=True)

    wfile_name = "clean_statement.csv"
    dest_path = sep.join([root_dir, read_fdr, wfile_name])
    df.to_csv(dest_path, index=False, encoding='utf8')
    print("\n\nTHE END")
