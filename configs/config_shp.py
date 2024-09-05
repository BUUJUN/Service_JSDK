# -*- coding: utf-8 -*-
"""
Created on 2024/09/04 09:59

@author: Bojun Wang
"""
import os
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import numpy as np
from joblib import Parallel, delayed

from configs import lon_interp, lat_interp

shp_info = {
	"paths" : {
		"nation": os.path.join(r"D:\statis_jiangsu\data\vectors\china_2017\nation.shp"),
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
	shp_sel = shp_nation[shp_nation['NAME']==region]
	if not len(shp_sel):
		shp_sel = shp_prov[shp_prov['NAME']==region]
	if not len(shp_sel):
		shp_sel = shp_city[shp_city['NAME']==region]
	if not len(shp_sel):
		shp_sel = shp_county[shp_county['NAME']==region]
	return shp_sel

shp_nation = read_shp(shp_info["paths"]["nation"])
shp_prov = read_shp(shp_info["paths"]["province"])
shp_city = read_shp(shp_info["paths"]["city"])
shp_county = read_shp(shp_info["paths"]["county"])

shp_concat = pd.concat([shp_nation,shp_prov,shp_city,shp_county]).reset_index(drop=True)

def mask_geopandas(gdf, lon:np.ndarray=lon_interp, lat:np.ndarray=lat_interp, col=None):
	"""
	使用 geopandas 对指定的 lon 和 lat 网格点生成对应的多边形属性掩码或布尔掩码。

	:param gdf: Geopandas GeoDataFrame，包含多边形数据
	:param lon: Xarray 数据中的经度
	:param lat: Xarray 数据中的纬度
	:param col: 需要返回的多边形的属性列名，若为 None 则返回布尔掩码
	:return: 返回多边形的属性值（例如 PCA）或布尔掩码
	"""
	# 将 lon 和 lat 转换为坐标点
	lon2d, lat2d = np.meshgrid(lon, lat)
	points = gpd.GeoSeries([Point(x, y) for x, y in zip(lon2d.ravel(), lat2d.ravel())])
	
	# 创建一个空数组用于存储属性值或布尔值
	mask = np.full(points.shape, np.nan, dtype=object)  # 属性掩码
	
	# 遍历每个多边形，检查点是否在该多边形内
	def fill_mask(polygon):
		contained_points = points.within(polygon.geometry)
		if col is None:
			mask[contained_points] = 1  # 布尔掩码只需要标记为 True
		else:
			mask[contained_points] = polygon[col]  # 属性掩码为对应的属性值
	
	Parallel(n_jobs=8)(delayed(fill_mask)(polygon) for _,polygon in gdf.iterrows())
	
	# 将掩码转换为与 lon 和 lat 一致的网格形状
	return mask.reshape(lon2d.shape)


