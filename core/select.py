# -*- coding: utf-8 -*-
"""
Created on 2024/09/03 17:19

@author: Bojun Wang
____ ____
选择统计区域
"""
import os
import xarray as xr
import numpy as np
import geopandas as gpd
import regionmask as rm
import warnings
from typing import Union

from configs import get_shp


def select_rect(data: Union[xr.Dataset, xr.DataArray], lon0: float, lon1: float, lat0: float, lat1: float):
	'''
	
	:param data:
	:param lon0: float
	:param lon1: float
	:param lat0: float
	:param lat1: float
	:return:
	'''
	lon0, lon1 = sorted([lon0, lon1])
	lat0, lat1 = sorted([lat0, lat1])
	
	data = data.sortby('lon').sortby('lat')
	
	data_sel = data.sel(lon=slice(lon0, lon1), lat=slice(lat0, lat1))
	
	return data_sel


def select_region(data: Union[xr.Dataset, xr.DataArray], region, dropna=True):
	shp_sel = get_shp(region)
	return select_shp(data, shp_sel, dropna=dropna)


def select_shp(data: Union[xr.Dataset, xr.DataArray], data_shp, dropna=True):
	'''
	
	:param data:
	:param data_shp:
	:param dropna:
	:return:
	'''
	data_mask = ~(rm.mask_geopandas(data_shp, data.lon, data.lat).isnull())
	data_sel = data.where(data_mask)
	if dropna:
		data_sel = data_sel.dropna(dim='lon', how='all').dropna(dim='lat', how='all')
	
	return data_sel


def select_points(data: Union[xr.Dataset, xr.DataArray], lonlat:np.ndarray):
	'''
	
	:param data:
	:param lonlat:
	:return:
	'''
	if lonlat.size < 1:
		return None
	
	lonlat = lonlat.squeeze()
	
	if len(lonlat.shape)>2:
		return None
	
	if lonlat.size == 2:
		lonlat = lonlat.reshape(-1, 2)
	
	if len(lonlat.shape) != 2:
		print("判断逻辑有误！")
		return None
		
	if lonlat.shape[-1] != 2:
		lonlat = lonlat.T
	
	lonlat_sel = []
	data_sel_list = []
	
	for lon, lat in lonlat:
		warnings.warn(f"`lon` ({lon}) < `lat` ({lat})", Warning)
		data_sing = data.sel(lon=lon, lat=lat, method='nearest')
		lonlat_sing = tuple([data_sing.lon.item(), data_sing.lat.item()])
		if not ({lonlat_sing} < set(lonlat_sel)):
			lonlat_sel.append(lonlat_sing)
			data_sel_list.append(data_sing)
	
	data_sel = xr.concat(data_sel_list, dim='station')
	
	return data_sel