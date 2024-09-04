# -*- coding: utf-8 -*-
"""
Created on 2024/09/04 09:59

@author: Bojun Wang
"""
import os
import geopandas as gpd
import pandas as pd

shp_info = {
	"paths" : {
		"province": os.path.join(r"D:\statis_jiangsu\data\vectors\china_2017\province.shp"),
		"city": os.path.join(r"D:\statis_jiangsu\data\vectors\china_2017\city.shp"),
		"county": os.path.join(r"D:\statis_jiangsu\data\vectors\china_2017\county.shp"),
	},
	"columns_rename" : {"名称": "NAME"},
	"columns" : ['OBJECTID', 'PAC', 'NAME', 'SHAPE_Leng', 'SHAPE_Area', 'geometry']
}

def read_shp(path):
	shp_data = gpd.read_file(path)
	renames = {name:shp_info['columns_rename'][name] for name in set(shp_info['columns_rename']) & set(shp_data.columns)}
	shp_data = shp_data.rename(columns=renames)
	shp_data = shp_data.loc[:, shp_info["columns"]]
	return shp_data

def get_shp(region):
	return shp_concat[shp_concat['NAME'] == region]

shp_concat = pd.concat([
	read_shp(shp_info["paths"]["province"]),
	read_shp(shp_info["paths"]["city"]),
	read_shp(shp_info["paths"]["county"]),
]).reset_index(drop=True)

shp_jiangsu = shp_concat[shp_concat.PAC.astype(str).str.startswith('320')].reset_index(drop=True)

