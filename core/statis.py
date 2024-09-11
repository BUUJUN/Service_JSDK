# -*- coding: utf-8 -*-
"""
Created on 2024/08/30 11:02

@author: Bojun Wang
____ ____
统计算法
"""
import warnings
import datetime

import numpy as np
import xarray as xr
import pandas as pd
from typing import Union

from configs import period_id
from typing import Union
from joblib import Parallel, delayed

from configs import shp_prov, shp_city
from configs import points_cz
from .read import Reader
from .read import select_shp
from .function import apply_dominant_dims

def statis_shp(reader: Reader, periods: list, data_shps: list = None, ):
	print("======== ========")
	print("统计行政区域 ...")
	
	if data_shps is None:
		data_shps = [shp_prov]
	
	data = reader.load_data()
	
	if data is None:
		return None
	
	statis = Statis_Period(data.time.data)
	
	res_list = []
	
	for data_shp in data_shps:
		print("region_info: \n", data_shp[["PAC", "NAME"]].head())
		
		region_info = data_shp[["PAC", "NAME"]].copy()
		region_info = region_info.set_index(["NAME"])
		region_unique = pd.concat([region_info.loc[[idx]].iloc[0].to_frame().T for idx in region_info.index.unique()])
		region_unique = region_unique.reset_index()
		region_unique = region_unique.rename(columns={'PAC': 'region_id', 'index': 'region'})
		region_unique['region_id'] = region_unique['region_id'].astype(int)
		# print(region_unique)
		
		data_mask = select_shp(data, data_shp, col='NAME')
		data_statis = data_mask.groupby('region').mean().load()
		if 'wd10_2' in data_mask.data_vars:
			data_statis['wd10_2'] = data_mask.wd10_2.load().groupby('region')\
				.apply((lambda da: apply_dominant_dims(data=da, dims=['stacked'])))
		
		for period in periods:
			data_p = statis.statis_specific(data_statis, period, avg=False)
			data_p = data_p.to_dataframe().reset_index()
			data_p = pd.merge(data_p, region_unique, on=['region'], how='left')
			res_list.append(data_p)
	
	res_df = pd.concat(res_list).reset_index(drop=True)
	
	columns_new = ["region", "region_id", "period_id", "time"]
	columns_new = columns_new + [col for col in res_df.columns if col not in columns_new]
	res_df = res_df[columns_new]
	
	print("统计完成")
	print("======== ========")
	
	return res_df


def statis_region(reader: Reader, periods: list, regions: list = None, ):
	print("======== ========")
	print("统计行政区域 ...")
	
	if regions is None:
		regions = list(pd.concat([shp_prov, shp_city]).NAME.unique())
	
	def statis_region_p(region):
		data_region = reader.load_data_region(region)
		if data_region is None: return None
		data_region = data_region.load()
		
		statis = Statis_Period(data_region.time.data)
		
		res_list_p = []
		for period in periods:
			data_p = statis.statis_specific(data_region, period, avg=False)
			data_p['region_id'] = data_p.attrs['region_id']
			data_p = data_p.to_dataframe().reset_index()
			res_list_p.append(data_p)
		res_df_p = pd.concat(res_list_p)
		
		return res_df_p
	
	res_list = [statis_region_p(reg) for reg in regions]
	# res_list = Parallel(n_jobs=-1)(delayed(statis_region_p)(reg) for reg in regions)
	res_list = [x for x in res_list if x is not None]
	if len(res_list)==0: return None
	
	res_df = pd.concat(res_list).reset_index(drop=True)
	
	columns_new = ["region", "region_id", "period_id", "time"]
	columns_new = columns_new + [col for col in res_df.columns if col not in columns_new]
	res_df = res_df[columns_new]
	
	print("统计完成")
	print("======== ========")
	
	return res_df


def statis_points(reader: Reader, periods: list, points: np.ndarray = points_cz):
	print("======== ========")
	print("统计场站点位 ...")
	
	data = reader.load_data_points(points)
	if data is None: return None
	data = data.load()
	
	statis = Statis_Period(data.time.data)
	
	res_list = []
	for period in periods:
		data_p = statis.statis_specific(data, period, avg=False)
		data_p = data_p.reset_coords().to_dataframe()
		res_list.append(data_p)
	res_df = pd.concat(res_list).reset_index()
	
	columns_new = ["station", "lon", "lat", "period_id", "time"]
	columns_new = columns_new + [col for col in res_df.columns if col not in columns_new]
	res_df = res_df[columns_new]
	
	print("统计完成")
	print("======== ========")
	
	return res_df


class Statis_Period(object):
	def __init__(self, time: np.ndarray, ):
		self.time = time
		self.year = pd.to_datetime(time).to_period('Y').to_timestamp()
		self.quarter = pd.to_datetime(time).to_period('M').astype('period[Q-DEC]').to_timestamp()
		self.season = pd.to_datetime(time).to_period('M').astype('period[Q-NOV]').to_timestamp()
		self.month = pd.to_datetime(time).to_period('M').to_timestamp()
		self.day = pd.to_datetime(time).to_period('D').to_timestamp()
	
	def __getitem__(self, item):
		if item=='year': return self.year
		if item=='quarter': return self.quarter
		if item=='season': return self.season
		if item=='month': return self.month
		if item=='day': return self.day
		if item=='time': return self.time
		if item=='day': return self.day
	
	def get_group(self, data: Union[xr.Dataset, xr.DataArray], period):
		if period not in ['year', 'season', 'month', 'week', 'day', 'time']:
			warnings.warn(f"No attribute named '{period}' for grouping", Warning)
			return None
		
		if not (data.time.data==self.time).all():
			warnings.warn("输入的 data.time 与 time 不一致！", Warning)
			return None
		
		# 使用 operation 函数对分组数据进行操作
		data_group = data.groupby(data.time.copy(data=self[period]))
		
		return data_group
	
	def mean(self, data: Union[xr.Dataset, xr.DataArray], period, avg=True):
		data_statis = self.get_group(data, period).mean()
		if avg:
			data_statis = data_statis.mean(dim=list(set(data_statis.dims) - {'time'}))
		data_statis["period_id"] = period_id[period]
		return data_statis
	
	def sum(self, data: Union[xr.Dataset, xr.DataArray], period, avg=True):
		data_statis = self.get_group(data, period).sum()
		if avg:
			data_statis = data_statis.mean(dim=list(set(data_statis.dims) - {'time'}))
		data_statis["period_id"] = period_id[period]
		return data_statis
	
	def apply_operation(self, data: Union[xr.Dataset, xr.DataArray], operation, period, avg=True):
		data_statis = self.get_group(data, period).apply(operation)
		if avg:
			data_statis = data_statis.mean(dim=list(set(data_statis.dims) - {'time'}))
		data_statis["period_id"] = period_id[period]
		return data_statis
	
	
	def statis_specific(self, data: Union[xr.Dataset, xr.DataArray], period, avg=True):
		data_statis = self.get_group(data, period).mean()
		if "t2m" in data_statis.data_vars:
			tmax_d = self.get_group(data.t2m, "day").max()
			data_statis["tmax"] = self.get_group(tmax_d, period).mean()
			tmin_d = self.get_group(data.t2m, "day").max()
			data_statis["tmin"] = self.get_group(tmin_d, period).mean()
		
		if 'tp' in data_statis.data_vars:
			data_statis["epe"] = self.get_group(data.tp>=50*24, period).sum()
		
		if 'wd10' in data_statis.data_vars:
			data_statis["wd10_2"] = self.get_group(data.wd10_2, period).apply(lambda da: apply_dominant_dims(data=da, dims=['time']))
		
		if avg:
			data_statis = data_statis.mean(dim=list(set(data_statis.dims) - {'time'}))
		
		data_statis["period_id"] = period_id[period]
		
		return data_statis
	
	
def statis_from_day(dataframe:pd.DataFrame, date_start=None, date_end=None, periods:Union[list, str]=None):
	if periods is None: periods = ['month', 'quarter', 'year']
	if type(periods) is str: periods = [periods]
	
	print(f"计算 {periods} 统计量 ... ")
	
	df_day = dataframe.loc[dataframe.period_id==period_id["day"]].copy()
	
	if date_start is not None: date_start = df_day.time.min()
	if date_end is not None: date_end = df_day.time.max()
	
	df_day = df_day.loc[:, (df_day.time>=date_start) & (df_day.time<=date_end)]
	statis_info = Statis_Period(df_day.time.values)
	
	if len(df_day)==0:
		print("统计失败，来源'{period}'的数据为空")
		return dataframe
	
	dfs = []
	
	for period in periods:
		df_period = df_day.copy()
		df_period['time'] = statis_info[period]
		
		if {"region", "region_id"}<=set(df_period.columns):
			df_period = df_period.groupby(["region", "region_id", "time"]).mean()
			
		elif {"station"}<=set(df_period.columns):
			df_period = df_period.groupby(["station", "time"]).mean()
			
		else:
			print("统计失败, 列名中不包含 ('region','name') 或 ('station') ")
			return dataframe
		
		df_period.reset_index(inplace=True)
		df_period["period_id"] = period_id[period]
		
		dfs.append(df_period)
	
	df_concat = pd.concat(dfs, axis=0)
	return df_concat


def statis_from_txt(dtlist: Union[list, np.ndarray] = None, periods:Union[list, str]=None):
	
	if periods is None: periods = ['DAY', 'MON', 'QUARTER']
	if type(periods) is str: periods = [periods]
	periods = ['MON' if p.upper()=='MONTH' else p.upper() for p in periods]
	
	load_data = lambda reg, pds: Reader(dsname="skjc", dtlist=dtlist, region=reg, period=pds).load_data()
	dfs = [load_data(region, periods) for region in ['CITY', 'PROVINCE']]
	
	if 'YEAR' in periods:
		print("基于 MON 数据计算年统计量 ... ")
		dfs_p = [load_data(region, ['MON']) for region in ['CITY', 'PROVINCE']]
		dfs_p = [x for x in dfs_p if x is not None]
		if len(dfs_p)==0:
			print("来源'{period}'的数据为空，将尝试其他 period ")
			
			for p in ['QUARTER', 'WEEK', 'DAY']:
				print(f"尝试基于 {p} 数据计算年统计量 ... ")
				dfs_p = [load_data(region, [p]) for region in ['CITY', 'PROVINCE']]
				dfs_p = [x for x in dfs_p if x is not None]
				if len(dfs_p)!=0:
					print(f"成功获取到 {p} 的数据")
					break
		
		if len(dfs_p)==0:
			print("计算年统计量失败，缺少数据源！")
			
		else:
			df_year = pd.concat(dfs_p, axis=0)
			statis_info = Statis_Period(df_year.time.values)
			df_year['time'] = statis_info['year']
			df_year = df_year.groupby(["region", "region_id", "time"]).mean()
			df_year.reset_index(inplace=True)
			df_year["period_id"] = period_id['year']
			dfs.append(df_year)
	
	dfs = [x for x in dfs if x is not None]
	
	if len(dfs)==0:
		return None
	
	df_concat = pd.concat(dfs, axis=0)
	
	return df_concat

	
if __name__ == '__main__':
	statis_from_txt([datetime.datetime(2023, 10, 18)], periods=['DAY'])