# -*- coding: utf-8 -*-
"""
Created on 2024/08/30 11:02

@author: Bojun Wang
____ ____
统计算法
"""
import warnings

import numpy as np
import xarray as xr
import pandas as pd
from typing import Union


class Statis_Period(object):
	def __init__(self, time: np.ndarray, ):
		self.time = time
		self.year = pd.to_datetime(time).to_period('Y').astype('datetime64[ns]')
		self.season = pd.to_datetime(time).to_period('M').astype('period[Q-NOV]').astype('str')
		self.month = pd.to_datetime(time).to_period('M').astype('datetime64[ns]')
		self.week = pd.to_datetime(time).strftime('%Y-%U')
		self.day = pd.to_datetime(time).to_period('D').astype('datetime64[ns]')
	
	def __getitem__(self, item):
		if item=='year': return self.year
		if item=='season': return self.season
		if item=='month': return self.month
		if item=='week': return self.week
		if item=='day': return self.day
		if item=='time': return self.time
		if item=='day': return self.day
	
	def apply_operation(self, data: Union[xr.Dataset, xr.DataArray], period, operation, avg=False):
		if period not in ['year', 'season', 'month', 'week', 'day', 'time']:
			warnings.warn(f"No attribute named '{period}' for grouping", Warning)
			return None
		
		if not (data.time.data==self.time).all():
			warnings.warn("输入的 data.time 与 time 不一致！", Warning)
			return None
		
		# 使用 operation 函数对分组数据进行操作
		data_statis = data.groupby(data.time.copy(data=self[period])).map(operation)
		
		if avg:
			data_statis = data_statis.mean(dim=list(set(data_statis.dims) - {'time'}))
		
		return data_statis
	
	def mean(self, data: Union[xr.Dataset, xr.DataArray], period, avg=True):
		return self.apply_operation(data, period, lambda array:array.mean(dim="time"), avg=avg)
	
	def sum(self, data: Union[xr.Dataset, xr.DataArray], period, avg=True):
		return self.apply_operation(data, period, lambda array:array.sum(dim="time"), avg=avg)


