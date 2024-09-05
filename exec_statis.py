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
	
	statis = Statis_Period(data.time.data)
	
	# 行政区划统计
	region_info = list(set(tuple(row) for _,row in shp_jiangsu.loc[:, ["PAC", "NAME"]].iterrows()))
	region_select = [select_shp(data, shp_jiangsu[shp_jiangsu.PAC == pac]) for pac,_ in region_info]
	
	res_dict = {
		period:[statis.mean(sel, period, avg=True) for sel in region_select] for period in periods
	}
	
	for period in res_dict:
		data_p = res_dict[period]
		data_p = [data.expand_dims({
			"id": [region_info[i][0]], "name": [region_info[i][1]]
		}).to_dataframe() for i, data in enumerate(data_p) if data]
		
		data_p = [x for x in data_p if x is not None]
		
		if not data_p:
			res_dict.update({period: None})
			continue
		
		data_p = pd.concat(data_p)
		
		data_p = data_p.reset_index()
		
		columns = \
			["id", "name", "time"] + \
			[col for col in data_p.columns if col not in ["id", "name", "time"]]
		
		data_p = data_p[columns]
		
		res_dict.update({period: data_p})
		
	with pd.ExcelWriter('statics_region.xlsx') as writer:
		[res_dict[period].to_excel(writer, sheet_name=period) for period in res_dict]
	
	return res_dict


def statis_points(data: Union[xr.DataArray, xr.Dataset], periods:list):
	statis = Statis_Period(data.time.data)
	# 站点统计
	points_select = select_points(data, points)
	
	res_dict = {
		period:statis.mean(points_select, "month", avg=False) for period in periods
	}
	
	for period in res_dict:
		data_p = res_dict[period]
		data_p = data_p.reset_coords().to_dataframe().reset_index()
		
		columns = \
			["station", "lon", "lat", "time"] + \
			[col for col in data_p.columns if col not in ["station", "lon", "lat", "time"]]
		
		data_p = data_p[columns]
		
		res_dict.update({period: data_p})
	
	with pd.ExcelWriter('statics_points.xlsx') as writer:
		[res_dict[period].to_excel(writer, sheet_name=period) for period in res_dict]
	
	return res_dict


if __name__ == '__main__':
	
	time_range = pd.date_range(start='2023-01-01T00', end='2023-12-31T23', freq='h').values
	
	reader = Reader(dsname="1km", dtlist=time_range, varlist=['tp', 't2m'])
	ds_load = reader.load_data()
	
	region_df = statis_region(ds_load, periods=["month", "season", "year"])
	points_df = statis_points(ds_load, periods=["month", "season", "year"])