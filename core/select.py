# -*- coding: utf-8 -*-
"""
Created on 2024/09/03 17:19

@author: Bojun Wang
____ ____
选择统计区域
"""
import xarray as xr
import numpy as np
from typing import Union
import regionmask as rm

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


def select_region(data: Union[xr.Dataset, xr.DataArray], region, col=None):
	shp_sel = get_shp(region)
	# print(shp_sel)
	return select_shp(data, shp_sel, col=col)


def select_shp(data: Union[xr.Dataset, xr.DataArray], data_shp, col=None):
	'''
	根据 shape 文件对数据进行切片，并可选删除无效数据。
	:param data: xarray 数据集（Dataset）或数据数组（DataArray）
	:param data_shp: 地理区域 shapefile
	:param col:
	:return: 切片后的数据
	'''
	groups = int(np.ceil(len(data_shp) / 31))
	
	data_shp_split = []
	
	for i in range(groups):
		if i<groups - 1:
			data_shp_split.append(data_shp.iloc[int(i * 31):int((i + 1) * 31)])
		else:
			data_shp_split.append(data_shp.iloc[int(i * 31):])
	
	mask = rm.mask_geopandas(data_shp_split[0], data.lon, data.lat)
	
	if groups>1:
		for data_shp_i in data_shp_split[1:]:
			mask = xr.where(np.isnan(mask), rm.mask_geopandas(data_shp_i, data.lon, data.lat), mask)
	
	if (col is not None) and (col in data_shp.columns):
		replace_func = np.vectorize(lambda x: data_shp[col].loc[x] if ~np.isnan(x) else np.nan)
		mask = mask.copy(data=replace_func(mask).astype(object))
	
	mask = mask.chunk({'lat': data.chunks['lat'], 'lon': data.chunks['lon']})
	data = data.assign_coords({"region": mask})
	return data


def select_points(data: Union[xr.Dataset, xr.DataArray], lonlat: np.ndarray):
	'''

	:param data:
	:param lonlat:
	:return:
	'''
	if lonlat.size<1:
		return None
	
	lonlat = lonlat.squeeze()
	
	if len(lonlat.shape)>2:
		return None
	
	if lonlat.size==2:
		lonlat = lonlat.reshape(-1, 2)
	
	if len(lonlat.shape)!=2:
		print("判断逻辑有误！")
		return None
	
	if lonlat.shape[-1]!=2:
		lonlat = lonlat.T
	
	lonlat_sel = []
	data_sel_list = []
	
	for lon, lat in lonlat:
		# warnings.warn(f"`lon` ({lon}) < `lat` ({lat})", Warning)
		data_sing = data.sel(lon=lon, lat=lat, method='nearest')
		lonlat_sing = tuple([data_sing.lon.item(), data_sing.lat.item()])
		if not ({lonlat_sing}<set(lonlat_sel)):
			lonlat_sel.append(lonlat_sing)
			data_sel_list.append(data_sing)
	
	data_sel = xr.concat(data_sel_list, dim='station')
	
	return data_sel