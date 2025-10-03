import phase1_etl as ph1
import phase2_daily_summary as ph2_ds
import phase2_daily_long_summary as ph2_ls
import phase2_daily_short_summary as ph2_ss
import phase2_ratios_and_kpi as ph2_rk
import data_prep as dtp
import el_cleansed_data as ecd

def app():

    fdr_path = "/Users/osagieaib/Library/CloudStorage/OneDrive-GodsVisionEnterprise/Documents/Workspace/IT Career/Cedarstone"
    file_name = "axi_trades.xlsx"
    acct_selected = 4728846
    # acct_selected = 6686814
    ph1.run_app()
    ph2_ds.run_app(parent_folder_path=fdr_path,
                   file_name=file_name,
                   acct_nr=acct_selected)
    # ph2_ls.run_app(run_dialog=False,
    #                acct_nr=acct_selected)
    # ph2_ss.run_app(run_dialog=False,
    #                acct_nr=acct_selected)
    ph2_rk.run_app(acct_nr=acct_selected)

if __name__ == "__main__":
    app()