# -*- coding: utf-8 -*-
"""
Created on 2024/09/04 09:10

@author: Bojun Wang
"""
import pandas as pd
import numpy as np
import xarray as xr
from typing import Union
from joblib import Parallel, delayed

from configs import shp_prov, shp_city
from configs import points_cz
from core import Reader, select_shp, Statis_Period

import warnings

warnings.filterwarnings('ignore')


def statis_shp(reader: Reader, periods: list, data_shps: list = None, ):
	print("统计行政区域 ...")
	
	if data_shps is None: data_shps = [shp_prov, shp_city]
	
	data = reader.load_data()
	if data is None: return None
	
	statis = Statis_Period(data.time.data)
	
	res_list = []
	for data_shp in data_shps:
		print("region_info: \n", data_shp[["PAC", "NAME"]].head())
		region_name = data_shp[["PAC", "NAME"]].copy()
		region_name = region_name.set_index(["PAC"])
		region_unique = pd.concat([region_name.loc[[idx]].iloc[0].to_frame().T for idx in region_name.index.unique()])
		region_unique = region_unique.reset_index()
		region_unique.columns = ['region', 'name']
		region_unique['region'] = region_unique['region'].astype(int)
		
		region_data = select_shp(data, data_shp, col='PAC').groupby("region").mean().load()
		region_data["region"] = region_data["region"].astype(int)
		# print(region_data)
		
		for period in periods:
			data_p = statis.mean(region_data, period, avg=False)
			data_p = data_p.to_dataframe().reset_index()
			data_p = pd.merge(data_p, region_unique, on=['region'], how='left')
			res_list.append(data_p)
	res_df = pd.concat(res_list).reset_index()
	
	columns_new = ["name", "region", "period_id", "time"]
	columns_new = columns_new + [col for col in res_df.columns if col not in columns_new]
	res_df = res_df[columns_new]
	
	return res_df


def statis_region(reader: Reader, periods: list, regions: list = None, ):
	print("统计行政区域 ...")
	
	if regions is None:
		regions = list(pd.concat([shp_prov, shp_city]).NAME.unique())
	
	def statis_region_p(region):
		data_region = reader.load_data_region(region)
		if data_region is None: return None
		data_region = data_region.load()
		data_region["region"] = data_region["region"].astype(int)
		
		statis = Statis_Period(data_region.time.data)
		
		res_list_p = []
		for period in periods:
			data_p = statis.mean(data_region, period, avg=False)
			data_p = data_p.to_dataframe().reset_index()
			data_p['name'] = region
			res_list_p.append(data_p)
		res_df_p = pd.concat(res_list_p)
		
		return res_df_p
	
	res_list = [statis_region_p(region) for region in regions]
	res_list = [x for x in res_list if x is not None]
	if len(res_list) == 0: return None
	
	res_df = pd.concat(res_list).reset_index()
	
	columns_new = ["name", "region", "period_id", "time"]
	columns_new = columns_new + [col for col in res_df.columns if col not in columns_new]
	res_df = res_df[columns_new]
	
	return res_df


def statis_points(reader: Reader, periods: list, points: np.ndarray = points_cz):
	print("统计场站点位 ...")
	
	data = reader.load_data_points(points)
	if data is None: return None
	data = data.load()
	
	statis = Statis_Period(data.time.data)
	
	res_list = []
	for period in periods:
		data_p = statis.mean(data, period, avg=False)
		data_p = data_p.reset_coords().to_dataframe()
		res_list.append(data_p)
	res_df = pd.concat(res_list).reset_index()
	
	columns_new = ["station", "lon", "lat", "period_id", "time"]
	columns_new = columns_new + [col for col in res_df.columns if col not in columns_new]
	res_df = res_df[columns_new]
	
	return res_df


if __name__=='__main__':
	time_range = pd.date_range(start='2023-01-01T00', end='2023-12-31T23', freq='h').values
	
	reader_1km = Reader(dsname="1km", dtlist=time_range, varlist=['t2m', 'r2m', 'tp', 'ws10', 'wd10', 'sp'])
	
	try:
		df_region = statis_region(reader=reader_1km, periods=["day"])
		with pd.ExcelWriter('statics_region.xlsx', engine='openpyxl') as writer:
			df_region.to_excel(writer, index=False)
	except Exception as e:
		print(e)
	
	try:
		df_points = statis_points(reader=reader_1km, periods=["month", "year"])
		with pd.ExcelWriter('statics_points.xlsx', engine='openpyxl') as writer:
			df_points.to_excel(writer, index=False)
	except Exception as e:
		print(e)
