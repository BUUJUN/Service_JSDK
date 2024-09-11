# -*- coding: utf-8 -*-
"""
Created on 2024/09/10 09:44

@author: Bojun Wang
"""
import datetime
import pandas as pd
from typing import Union
import xarray as xr
import numpy as np

def dominant(array:np.ndarray):
	array = (array[~np.isnan(array)]).astype(int).ravel()
	array = array[array>=0]
	
	array_count = np.bincount(array)
	array_argmax = np.argmax(array_count)
	
	return array_argmax
	

def apply_dominant_dims(data: Union[xr.Dataset, xr.DataArray], dims:list=None):
	if dims is None:
		dims = ['time']
	
	if 'stacked' in dims:
		dims.remove('stacked')
		dims.extend([dim for dim in data.dims if 'stacked_' in dim])
	
	count_res = xr.apply_ufunc(dominant, data, input_core_dims=[dims], output_core_dims=[[]], vectorize=True)
	return count_res

def convert_time(time, days=1):
	time = time + datetime.timedelta(days=-1)
	time_begin = pd.to_datetime(time.date())
	time_end = time_begin + datetime.timedelta(days=days) + datetime.timedelta(hours=-1)
	time_series = pd.date_range(time_begin, time_end, freq='h')
	return time_series