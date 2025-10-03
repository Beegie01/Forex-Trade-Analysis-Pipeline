import copy
import pandas as pd
from helper_utils import HelperUtils as hu
import os
import shutil


def run_app(parent_folder_path: str,
            file_name: str="axi_trades.xlsx"):

    # configure display settings for pandas output
    hu.configure_display_settings()

    # create folder for source data
    root_path = parent_folder_path
    # you can change root_path value to the folder containing the custom scripts
    read_fdr = "SourceData"
    sep = "/"
    # extract source data into dataframe
    dest_path = sep.join([root_path,
                          file_name])
    src_df = pd.read_excel(dest_path)
    # print(src_df.info())
    # print(src_df.head(5))
    df = copy.deepcopy(src_df)
    # print(df.head())
    # get account id from dataset
    col_list = ['AxiCorp Financial Services Pty Ltd']
    cond = df[col_list[0]].astype(str).str.contains("Account:")
    acct_nr = int(df.loc[cond, col_list[0]].astype(str).str.split(": ").str[-1].str.strip().iloc[0])
    # print(acct_nr)

    # copy each selected file to target folder
    src_fname = dest_path.split(sep)[-1]
    src_filename = f"{acct_nr}_{src_fname}"
    dest_fname = sep.join([root_path,
                           read_fdr,
                           src_filename])
    # copy source data to new file name
    src_path = sep.join([root_path,
                          read_fdr])
    os.makedirs(src_path, exist_ok=True)
    shutil.copy2(dest_path, dest_fname)
    sample_fname = src_filename

    # collate name:path for all files in source folder
    filenames_dict = hu.get_full_path(root_path, specify_ftype=['xls', 'ods'])
    print(filenames_dict)
    # filenames_dict = {v:k for k,v in filenames_dict.items()}
    # print(filenames_dict)

    if (len(sample_fname) < 1) and (len(filenames_dict)>0):
        sample_fname = list(filenames_dict.keys())[0]
        # print(sample_fname)
    else:
        print("\n\nSOURCE FILE NOT AVAILABLE\n"
              "DATA CLEANING ABORTED!\n")
        return

    sample_df = hu.cleaning_steps(filenames_dict[sample_fname])

    # drop model filename from dictionary
    filenames_dict.pop(sample_fname)

    # append remaining file data to sample data
    df = hu.append_to_sample_df(sample_df,
                                filenames_dict,
                                use_func="cleaning")

    # print(df.info())
    # print(df['Account Number'].value_counts())
    # print(df.tail())
    # sort data by account and datetime
    sort_cols = ["Account Number",
                 "Ticket"]
    final_df = df.sort_values(by=sort_cols)
    # create folder if it does not exist
    read_fdr = "CleansedData"
    wfile_name = "axi_dataset_cleaned.csv"
    dest_path = sep.join([root_path, read_fdr])
    os.makedirs(dest_path, exist_ok=True)

    # write date to filesystem
    dest_path = sep.join([root_path, read_fdr, wfile_name])
    final_df.to_csv(path_or_buf=dest_path,
              index=False,
              encoding='utf8')
    print("\nDATA CLEANING FINISH!\n")
