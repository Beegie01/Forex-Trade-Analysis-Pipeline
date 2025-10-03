import data_prep as dtp
import el_cleansed_fact as ecf
import el_cleansed_dim as ecd
import el_transformed_dw_data as etd

def app():

    dtp.run_app(run_dialog=True)
    ecd.run_app()
    ecf.run_app()
    etd.run_app()

if __name__ == "__main__":
    app()