import pandas as pd
from wbe.constants import DATA_PATH

def get_cdc_wbe_data():
    url =  "https://data.cdc.gov/api/views/j9g8-acpt/rows.csv?accessType=DOWNLOAD"
    data = pd.read_csv(url, index_col="sample_collect_date")
    data.index = pd.to_datetime(data.index)
    data = data.sort_index()
    outdir = DATA_PATH / "wbe"
    DATA_PATH.mkdir(exist_ok=True)
    outdir.mkdir(exist_ok=True)
    data.to_csv(outdir / "cdc_data.csv")
    