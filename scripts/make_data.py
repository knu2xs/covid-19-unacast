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

# facilitate using local unacast package resources
sys.path.append(os.path.abspath('../src'))
import unacast

# paths to common data locations - NOTE: to convert any path to a raw string, simply use str(path_instance)
project_parent = Path('./').absolute().parent

data_dir = project_parent/'data'

data_raw = data_dir/'raw'
data_int = data_dir/'interim'
data_out = data_dir/'processed'

gdb_raw = data_raw/'raw.gdb'
gdb_int = data_int/'interim.gdb'
gdb_out = data_out/'processed.gdb'

# resource variables
unacast_csv_pth = data_raw/'covid_sds_full_2020-03-30_jm.csv'
itm_id = '7566e0221e5646f99ea249a197116605'
full_fc = gdb_out / 'unacast'
last_day_fc = gdb_out/'unacast_last_day'

# create an output feature class with the full dataset
full_df = unacast.create_update_dataframe(itm_id, unacast_csv_pth)
if arcpy.Exists(str(full_fc)):
    arcpy.management.Delete(str(full_fc))
full_df.spatial.to_featureclass(full_fc)

# filter to just the most recent day

