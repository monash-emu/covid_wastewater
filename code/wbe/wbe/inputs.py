import numpy as np
import pandas as pd
from wbe.constants import DATA_PATH


def get_cdc_wbe_data():
    url = "https://data.cdc.gov/api/views/j9g8-acpt/rows.csv?accessType=DOWNLOAD"
    data = pd.read_csv(url, index_col="sample_collect_date")
    data.index = pd.to_datetime(data.index)
    data = data.sort_index()
    outdir = DATA_PATH / "wbe"
    DATA_PATH.mkdir(exist_ok=True)
    outdir.mkdir(exist_ok=True)
    data.to_csv(outdir / "cdc_data.csv")


def split_concentration_var(
    data: pd.DataFrame,
) -> pd.DataFrame:
    """Get new columns for the liquid
    and solid PCR concentrations.

    Args:
        data: The raw data loaded by get_cdc_wbe_data

    Returns:
        The data with the additional columns
    """

    # Get masks
    log_mask = data["pcr_target_units"] == "log10 copies/l wastewater"
    linear_mask = ~log_mask
    solid_mask = data["pcr_target_units"] == "copies/g dry sludge"
    liquid_mask = ~solid_mask

    # Three possible values in the dataset
    log_liquid = log_mask & liquid_mask
    lin_liquid = linear_mask & liquid_mask
    lin_solid = linear_mask & solid_mask

    # Initialise new columns
    data.loc[:, "liquid_pcr_conc"] = np.nan
    data.loc[:, "solid_pcr_conc"] = np.nan

    # Fill
    data.loc[lin_liquid, "liquid_pcr_conc"] = data.loc[
        lin_liquid, "pcr_target_avg_conc"
    ]
    data.loc[log_liquid, "liquid_pcr_conc"] = (
        10.0 ** data.loc[log_liquid, "pcr_target_avg_conc"]
    )
    data.loc[lin_solid, "solid_pcr_conc"] = data.loc[lin_solid, "pcr_target_avg_conc"]

    return data


group_vars = ["sewershed_id", "sample_collect_date"]


def group_data(
    sample_type: str,
    data: pd.DataFrame,
) -> pd.DataFrame:
    """Get median data over shed and collection date.

    Args:
        sample_type: Whether liquid or solid
        data:

    Returns:
        _description_
    """
    grouped_obs = (
        data.groupby(group_vars, as_index=False).agg(
            pcr_conc=(f"{sample_type}_pcr_conc", "median"),
            n_raw_rows=(f"{sample_type}_pcr_conc", "size"),
        )
    ).sort_values(group_vars)
    grouped_obs.index = pd.to_datetime(grouped_obs["sample_collect_date"])
    cols = [c for c in grouped_obs.columns if c != "sample_collect_date"]
    return grouped_obs[cols]
