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

from configs import shp_prov, shp_city, period_id
from configs import points_cz
from core import Reader, select_shp, Statis_Period

import warnings

warnings.filterwarnings('ignore')


def statis_fday(df, periods: Union[list, str] = None):
	if periods is None: periods = ['month', 'year']
	if type(periods) is str: periods = [periods]
	
	print(f"计算 {periods} 统计量 ... ")
	
	df_day = df.loc[df.period_id==period_id["day"]].copy()
	
	if len(df_day)==0:
		print("统计失败，来源'{period}'的数据为空")
		return df
	
	dfs = [df]
	
	for period in periods:
		df_period = df_day.copy()
		if period=="year":
			df_period["time"] = df_period.time.str[:4]
		
		elif period=="month":
			df_period["time"] = df_period.time.str[:7]
		
		elif period=="week":
			df_period['time'] = pd.to_datetime(df_period.time, format='%Y-%m-%d')
			df_period["time"] = df_period.time.dt.strftime('%YW%w')
		
		elif period=="quarter":
			df_period['time'] = pd.to_datetime(df_period.time, format='%Y-%m-%d')
			df_period['time'] = df_period.time.dt.to_period('Q').astype(str)
		
		if {"region", "name"}<=set(df_period.columns):
			df_period = df_period.groupby(["time", "region", "name"]).mean()
		elif {"station"}<=set(df_period.columns):
			df_period = df_period.groupby(["time", "station"]).mean()
		else:
			print("统计失败, 列名中不包含 ('region','name') 或 ('station') ")
			return df
		df_period["period_id"] = period_id[period]
		
		dfs.append(df_period)
	
	df_concat = pd.concat(dfs, axis=0).reset_index(drop=True)
	return df_concat


if __name__=='__main__':
	try:
		df_region_day = pd.read_excel("statics_region.xlsx")
		df_region_statis = statis_fday(df_region_day)
		with pd.ExcelWriter('statics_region_from_day.xlsx', engine='openpyxl') as writer:
			df_region_statis.to_excel(writer, index=False)
	except Exception as e:
		print(e)
	
	try:
		df_points_day = pd.read_excel("statics_points.xlsx")
		df_points_statis = statis_fday(df_points_day)
		with pd.ExcelWriter('statics_points_from_day.xlsx', engine='openpyxl') as writer:
			df_points_statis.to_excel(writer, index=False)
	except Exception as e:
		print(e)
