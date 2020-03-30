#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
from pathlib import Path

from arcgis.features import GeoAccessor
from arcgis.gis import GIS
import janitor
import pandas as pd

__all__ = ['get_county_geometry_series', 'load_unacast_csv', 'create_update_dataframe']

# paths to common data locations - NOTE: to convert any path to a raw string, simply use str(path_instance)
project_parent = Path('./').absolute().parent

data_dir = project_parent / 'data'

data_raw = data_dir / 'raw'
data_int = data_dir / 'interim'
data_out = data_dir / 'processed'

gdb_raw = data_raw / 'raw.gdb'
gdb_int = data_int / 'interim.gdb'
gdb_out = data_out / 'processed.gdb'


def get_county_geometry_series(itm_id: [Path, str]):
    """
    Get a Pandas Series with the county FIPS as a string index, and the values as a valid geometry
        column from an online feature service.
    :return: Pandas Series.
    """
    # Get a layer instance
    cty_lyr = GIS().content.get(itm_id).layers[0]

    # retrieve the data necessary
    cty_df = cty_lyr.query(out_fields=['FIPS'], out_sr=4326, as_df=True)

    # set the index and slice off only the geometry column to return as a series
    cty_df.set_index(['FIPS'], inplace=True)
    cty_srs = cty_df['SHAPE']
    return cty_srs


def load_unacast_csv(unacast_csv_path: [Path, str], existing_data_end_date: datetime = None) -> pd.DataFrame:
    """
    Load the input CSV from Unacast, ensure the fields are correctly typed, and remove the location
        field.
    :return:
    """
    # read in the data and clean up the field names
    una_df = pd.read_csv(unacast_csv_path).clean_names()

    # ensure the fields are cleaned up
    una_df.county_fips = una_df.county_fips.astype(str).str.zfill(5)
    una_df.last_updated = pd.to_datetime(una_df.last_updated)
    una_df.localeventdate = pd.to_datetime(una_df.localeventdate)

    # remove the unneeded county colun
    una_df.drop(columns=['county_centroid'], inplace=True)

    # if a date is already provided, filter to only records newer than the date
    if existing_data_end_date:
        una_df = una_df[una_df['localeventdate'] > existing_data_end_date].copy()

    return una_df


def create_update_dataframe(unacast_csv_path: [Path, str], existing_data_end_date: datetime = None) -> pd.DataFrame:
    """
    On stop shopping to create a Spatially Enabled Dataframe with Unacast data and county polygon
        geometry.
    :return: Spatially enabled dataframe
    """
    # get the county series and unacast dataframes
    county_srs = get_county_geometry_series()
    una_df = load_unacast_csv(unacast_csv_path, existing_data_end_date)

    # combine the unacaset human movement dataframe with the polygon geometry
    full_df = una_df.join(county_srs, on='county_fips')

    return full_df

def create_unacast_feature_class(unacast_csv_path: [Path, str], output_feature_class: [Path, str] = gdb_out/'unacast') -> Path:
    """
    Create an output feature class of county polygons with Unacast data.
    :param unacast_csv_path: Path to Unacast csv.
    :param output_feature_class: Path where to stor
    """