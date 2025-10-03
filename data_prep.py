import copy
import pandas as pd
from helper_utils import HelperUtils as hu
import os
import shutil


def run_app(run_dialog=True):

    # configure display settings for pandas output
    hu.configure_display_settings()

    root_path = "/Users/osagieaib/Library/CloudStorage/OneDrive-GodsVisionEnterprise/Documents/Workspace/IT Career/Cedarstone"
    # create folder for source data
    # root_path = os.getcwd()
    # you can change root_path value to the folder containing the custom scripts
    read_fdr = "SourceData"
    sep = "/"
    # create account subfolder if it does not exist
    dest_path = sep.join([root_path, read_fdr])
    os.makedirs(dest_path, exist_ok=True)

    selected_file_list = ()
    if run_dialog:
        # copy selected source file to source folder
        prompt_text = 'SELECT SOURCE FILE'
        selected_file_list = hu.select_files_dialog(starting_loc=root_path,
                                                    prompt_text=prompt_text)
        # print(type(selected_file_list))

    acct_details = {}
    sample_fname = ""

    if isinstance(selected_file_list, tuple) and len(selected_file_list)>0:
        for i in range(len(selected_file_list)):
            cur_fpath = selected_file_list[i]
            src_fname = cur_fpath.split(sep)[-1]

            # extract source data into dataframe
            src_df = pd.read_excel(cur_fpath)
            # print(src_df.info())
            # print(src_df.head(5))
            df = copy.deepcopy(src_df)
            # print(df.head())
            # get account id from dataset
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

            # copy each selected file to target folder
            src_filename = f"{acct_details['acct_nr'][0]}_{src_fname}"
            dest_fname = sep.join([root_path, read_fdr, src_filename])
            # copy source data to new file name
            shutil.copy2(cur_fpath, dest_fname)
            if i == 0:  # use the first file as the model structure
                sample_fname = src_filename

    # collate name:path for all files in source folder
    filenames_dict = hu.get_full_path(dest_path, specify_ftype=['xls', 'ods'])
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

    clean_obj_dict = hu.prep_steps(filenames_dict[sample_fname])
    sample_df = clean_obj_dict['fact_df']
    dim_acct = clean_obj_dict['dim_account']
    # print(sample_df.info())
    # print(dim_acct)

    # drop model filename from dictionary
    filenames_dict.pop(sample_fname)

    # append remaining file data to sample data
    df = hu.append_to_sample_df(sample_df,
                                filenames_dict,
                                use_func="prep")

    # print(df.info())
    # print(df['Account_Number'].value_counts())
    # print(df.tail())

    # sort data by account and datetime
    sort_cols = ["Account_Number",
                 "Ticket"]
    df = df.sort_values(by=sort_cols)

    # convert columns to lower case
    df.columns = map(str.lower, list(df.columns))
    final_df = df
    # print(final_df.info())

    # append remaining file data to sample dim data
    dim_df = hu.append_to_sample_df(dim_acct,
                                    filenames_dict,
                                    get_data='dt',
                                    use_func="prep")

    # print(dim_df.info())
    # print(dim_df['acct_nr'].value_counts())
    # print(dim_df.tail())

    # sort data by account and datetime
    sort_cols = ["acct_nr"]
    dim_df = dim_df.sort_values(by=sort_cols)

    # convert columns to lower case
    dim_df.columns = map(str.lower, list(dim_df.columns))
    final_dim_df = dim_df
    # print(final_dim_df.info())

    # create folder if it does not exist
    read_fdr = "CleansedData"
    ffile_name = "axi_dataset_cleaned_dw_fact.csv"
    dfile_name = "axi_dataset_cleaned_dw_dim.csv"
    dest_path = sep.join([root_path, read_fdr])
    os.makedirs(dest_path, exist_ok=True)

    # write date to filesystem
    dest_path = sep.join([root_path, read_fdr, ffile_name])
    final_df.to_csv(path_or_buf=dest_path,
              index=False,
              encoding='utf8')

    dest_path = sep.join([root_path, read_fdr, dfile_name])
    final_dim_df.to_csv(path_or_buf=dest_path,
                    index=False,
                    encoding='utf8')
    print("\nDATA CLEANING FINISH!\n")
