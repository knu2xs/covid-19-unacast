#!/usr/bin/env python
# coding: utf-8
"""
    Licensing
 
    Copyright 2020 Esri
 
    Licensed under the Apache License, Version 2.0 (the "License"); You
    may not use this file except in compliance with the License. You may
    obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
 
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
    implied. See the License for the specific language governing
    permissions and limitations under the License.
 
    A copy of the license is available in the repository's
    LICENSE file.
"""
import arcpy
from datetime import datetime
import os
import sys
from pathlib import Path
import pandas as pd

# facilitate using local unacast package resources
sys.path.append(os.path.abspath('../src'))
import unacast

# paths to common data locations - NOTE: to convert any path to a raw string, simply use str(path_instance)
project_parent = Path('./').absolute().parent

data_dir = os.path.join(project_parent, "data")

data_raw = os.path.join(data_dir, "raw")
data_int = os.path.join(data_dir, "interim")
data_out = os.path.join(data_dir, "processed")

gdb_raw = os.path.join(data_raw, "raw.gdb")
gdb_int = os.path.join(data_int, "interim.gdb")
gdb_out = os.path.join(data_out, "processed.gdb")

# Input variables (must pre-exist)
unacast_csv_path = os.path.join(data_raw, "covid_sds_full_2020_04_01.csv")
item_id = "7566e0221e5646f99ea249a197116605"
full_fc = os.path.join(gdb_out, "unacast")
last_day_fc = os.path.join(gdb_out, "unacast_last_day")
generalized_fc = os.path.join(gdb_out, "unacast_generalized")


# Get county series
print("Converting counties feature layer item to pandas series: {}".format(item_id))
county_series = unacast.get_county_geometry_series(item_id)
# Get Unacast data frame
print("Converting input csv to pandas data frame: {}".format(unacast_csv_path))
una_df = unacast.load_unacast_csv(unacast_csv_path, None)

# Create data frame of all Uncast data
print("Join county series to Unacast data frame")
full_df = una_df.join(county_series, on="county_fips", how="right")



# Create feautre class of all Unacast data
print("Create feature class of all Unacast data")
if arcpy.Exists(full_fc):
    arcpy.Delete_management(full_fc)
full_df.spatial.to_featureclass(full_fc)



# Create feautre class of last day's Unacast data
print("Create feature class of last day's Unacast data")
max_day_df = full_df[full_df.localeventdate == full_df.localeventdate.max()]
# Keep null values so that final output contains all counties
null_df = full_df[full_df.localeventdate.isnull()]
day_df = pd.concat([max_day_df, null_df])
if arcpy.Exists(last_day_fc):
    arcpy.Delete_management(last_day_fc)
day_df.spatial.to_featureclass(last_day_fc)



# Create feature class with each date as its own field by pivoting data frame
print("Pivot full data frame to create generalized data frame")
pivot_df = full_df.pivot(index="county_fips", columns="localeventdate", values=["grade_total", "grade_distance",
                                                                                        "grade_visitation",
                                                                                        "n_grade_total",
                                                                                        "n_grade_distance",
                                                                                        "n_grade_visitation"])
# Join pivoted data with county series
print("Join county series to pivoted data frame")
generalized_df = pivot_df.join(county_series, on="county_fips", how="right").clean_names()
# Drop field created by null localeventdate
generalized_df.drop(columns=["_grade_total_nat_"], inplace=True)
# Set geometry field
generalized_df.spatial.set_geometry("shape")
# Reset index to ensure county fips field is written to output feature class
generalized_df.reset_index(level=0, inplace=True)
print("Create feature class of generalized Unacast data")
if arcpy.Exists(generalized_fc):
    arcpy.Delete_management(generalized_fc)
generalized_df.spatial.to_featureclass(generalized_fc)
