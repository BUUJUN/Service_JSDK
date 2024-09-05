# -*- coding: utf-8 -*-
"""
Created on 2024/09/04 09:10

@author: Bojun Wang
"""
import pandas as pd
import numpy as np
import xarray as xr

from typing import Union

from configs import shp_jiangsu
from configs import points
from core import Reader, select_shp, select_points, Statis_Period

def statis_region(data:Union[xr.DataArray, xr.Dataset], periods:list):
	
	print("统计行政区域 ...")
	
	statis = Statis_Period(data.time.data)
	
	# 行政区划统计
	region_info = list(set(tuple(row) for _,row in shp_jiangsu.loc[:, ["PAC", "NAME"]].iterrows()))
	region_select = [select_shp(data, shp_jiangsu[shp_jiangsu.PAC == pac]) for pac,_ in region_info]
	
	res_list = []
	
	for period in periods:
		data_p = [statis.mean(sel, period, avg=True) for sel in region_select]
		data_p = [data.expand_dims({
			"id": [region_info[i][0]], "name": [region_info[i][1]]
		}).to_dataframe() for i, data in enumerate(data_p) if data]
		
		data_p = [x for x in data_p if x is not None]
		if not data_p:
			continue
		
		data_p = pd.concat(data_p)
		res_list.append(data_p)
		
	res_df = pd.concat(res_list).reset_index()
	
	columns = \
		["name", "id", "period_id", "time"] + \
		[col for col in res_df.columns if col not in ["name", "id", "period_id", "time"]]
	
	res_df = res_df[columns]
	
	with pd.ExcelWriter('statics_region.xlsx', engine='openpyxl') as writer:
		res_df.to_excel(writer, index=False)
	
	return res_df


def statis_points(data: Union[xr.DataArray, xr.Dataset], periods:list):
	
	print("统计场站点位 ...")
	
	statis = Statis_Period(data.time.data)
	# 站点统计
	points_select = select_points(data, points)
	
	res_list = []
	
	for period in periods:
		data_p = statis.mean(points_select, period, avg=False)
		data_p = data_p.reset_coords().to_dataframe()
		res_list.append(data_p)
	
	res_df = pd.concat(res_list).reset_index()
	
	columns = \
		["station", "lon", "lat", "period_id", "time"] + \
		[col for col in res_df.columns if col not in ["station", "lon", "lat", "period_id", "time"]]
	
	res_df = res_df[columns]
	
	with pd.ExcelWriter('statics_points.xlsx', engine='openpyxl') as writer:
		res_df.to_excel(writer, index=False)
	
	return res_df


if __name__ == '__main__':
	
	time_range = pd.date_range(start='2023-01-01T00', end='2023-12-31T23', freq='h').values
	
	reader = Reader(dsname="1km", dtlist=time_range, varlist=['t2m', 'r2m', 'tp', 'ws10', 'wd10', 'sp'])
	ds_load = reader.load_data()
	
	region_df = statis_region(ds_load, periods=["month", "season", "year"])
	points_df = statis_points(ds_load, periods=["month", "season", "year"])