# -*- coding: utf-8 -*-
"""
Created on 2024/08/30 11:21

@author: Bojun Wang
____ ____
读取数据，并做标准化处理
"""
from typing import Union
import numpy as np
import pandas as pd
import datetime
import xarray as xr
from joblib import Parallel, delayed
import warnings

from .select import select_shp, select_points, select_region
from configs import file_info, lat_interp, lon_interp, var_reattrs, var_renames, extract_files, extract_times, T0, period_id


class Reader(object):
	def __init__(self, dsname, dtlist: Union[list, np.ndarray] = None, varlist: list = None, **extract_kwargs):
		'''

		:param dsname:
		:param dtlist:
		:param varlist:
		:param extract_kwargs: vars:list
		'''
		self.dsname = dsname
		self.dtlist = dtlist.copy() if dtlist is not None else None
		self.varlist = varlist.copy() if varlist is not None else None
		self.file_path = file_info[dsname]['prefix']
		self.file_fmt = file_info[dsname]['format']
		self.file_type = file_info[dsname]['type']
		self.file_vars = file_info[dsname]['vars']
		self.file_list = extract_files(dsname, dtlist, varlist, **extract_kwargs)
		self.time_dict = extract_times(dsname, self.file_list)
		
		self.var_renames = var_renames[dsname]
		self.var_reattrs = var_reattrs
		
		self.lat_interp = lat_interp
		self.lon_interp = lon_interp
		
		print("———————— ————————")
		print(f"数据集: {dsname}")
		print(f"文件列表: {self.file_list}")
		print(f"时间列表: {self.time_dict.values()}")
	
	def read_data(self, filepath) -> Union[None, xr.Dataset, xr.DataArray, pd.DataFrame]:
		print(f"Read -> {filepath}")
		if self.file_type in ['nc', 'netcdf']:
			try:
				ds = xr.open_dataset(filepath, chunks={'lon': 80, 'lat': 70})
				ds = ds.rename({name: self.var_renames[name] for name in (set(self.var_renames) & set(ds.coords))})
				ds = ds.sortby('lon').sortby('lat')
				if not 'time' in ds.coords:
					ds = ds.expand_dims({'time': [self.time_dict[filepath]]})
				return ds
			except Exception as e:
				print(f"读取{filepath}错误", e)
				return None
		
		if self.file_type in ['txt']:
			if self.dsname in ['skjc']:
				try:
					df = pd.read_csv(filepath, sep='\s+')
					return df
				except Exception as e:
					print(e)
					return None
			
			else:
				print(f"数据集 '{self.dsname}' 的文件读取功能暂未开发！")
				return None
	
	def interp_data(self, ds, **kwargs):
		lon_i = self.lon_interp
		lat_i = self.lat_interp
		
		print(
			f"Interp -> lon: ({lon_i[0]:.2f},{lon_i[-1]:.2f},{lon_i[1] - lon_i[0]:.2f})",
			f"lat: ({lat_i[0]:.2f},{lat_i[-1]:.2f},{lat_i[1] - lat_i[0]:.2f})")
		
		if ds is None: return None
		try:
			ds_interp = ds.interp(lon=lon_i, lat=lat_i, method='linear', **kwargs).chunk({"lon": 80, "lat": 70})
			return ds_interp
		except Exception as e:
			print(f"插值{list(ds.data_vars)}错误", e)
			return None
	
	def standard_data(self, data: Union[pd.DataFrame, xr.Dataset]):
		print("Standarding ...")
		if type(data) is xr.Dataset:
			# 重命名变量名
			ds_standard = data.rename_vars({name: self.var_renames[name] for name in (set(self.var_renames) & set(data.data_vars))})
			
			if self.dsname in ['1km']:
				if 'tp' in ds_standard.data_vars:
					ds_standard['tp'] = ds_standard.tp * 24
				if 't2m' in ds_standard.data_vars:
					ds_standard['t2m'] = ds_standard.t2m - T0
				if 'd2m' in ds_standard.data_vars:
					ds_standard['d2m'] = ds_standard.d2m - T0
				if 'sp' in ds_standard.data_vars:
					ds_standard['sp'] = ds_standard.sp * 0.01
			
			if (self.varlist is None) or ('wd10' in self.varlist):
				if not 'wd10' in ds_standard.data_vars:
					print("根据 u10, v10 计算 wd10")
					if not ({'u10', 'v10'}<=set(ds_standard.data_vars)):
						print("缺少 u10 或 v10")
					else:
						u = ds_standard.u10
						v = ds_standard.v10
						ds_standard['wd10'] = u.copy(data=(270 - np.arctan2(v.data, u.data) * 180 / np.pi) % 360)
			
			[ds_standard[var].attrs.update(self.var_reattrs[var]) for var in set(self.var_reattrs) & set(ds_standard.variables)]
			
			ds_standard = ds_standard.sortby('time')
			
			dims_order = ['time'] + list(set(ds_standard.dims) - {'time'})
			ds_standard = ds_standard.transpose(*dims_order)
			
			if not (self.varlist is None):
				ds_standard = ds_standard.drop_vars(list(set(ds_standard.data_vars) - set(self.varlist)))
			
			return ds_standard
		
		elif self.dsname in ['skjc']:
			data.rename(columns={data.columns[0]: "name", data.columns[1]: "region"}, inplace=True)
			data.rename(columns={col: self.var_renames[col] for col in (set(data.columns) & set(self.var_renames))}, inplace=True)
			data.rename(columns={col: col.lower() for col in data.columns}, inplace=True)
			
			if 'tp' in data.columns:
				data['tp'] = data.tp * 24
			
			if "hour" in data.columns:
				data['time'] = data[["year", "mon", "day", "hour"]] \
					.apply(lambda row: f"{row.year}-{row.mon}-{row.day}T{row.hour}", axis=1)
				data['period_id'] = period_id['hour']
				data.drop(columns=["year", "mon", "day", "hour"], inplace=True)
			
			elif "day" in data.columns:
				data['time'] = data[["year", "mon", "day"]] \
					.apply(lambda row: f"{row.year}-{row.mon}-{row.day}", axis=1)
				data['period_id'] = period_id['day']
				data.drop(columns=["year", "mon", "day"], inplace=True)
			
			elif "week" in data.columns:
				data['time'] = data[["year", "week"]] \
					.apply(lambda row: f"{row.year}W{row.week}", axis=1)
				data['period_id'] = period_id['week']
				data.drop(columns=["year", "week"], inplace=True)
			
			elif "mon" in data.columns:
				data['time'] = data[["year", "mon"]] \
					.apply(lambda row: f"{row.year}-{row.mon}", axis=1)
				data['period_id'] = period_id['month']
				data.drop(columns=["year", "mon"], inplace=True)
			
			elif "quarter" in data.columns:
				data['time'] = data[["year", "quarter"]] \
					.apply(lambda row: f"{row.year}Q{row.quarter}", axis=1)
				data['period_id'] = period_id['quarter']
				data.drop(columns=["year", "quarter"], inplace=True)
			
			else:
				print("Error!")
				return None
			
			return data
	
	
	def load_data(self):
		
		print("———————— ————————")
		print(f"Loading Data")
		
		if self.file_type in ['nc', 'netcdf']:
			def preprocess(fn):
				ds = self.read_data(fn)
				if ds is None:
					return None
				else:
					return self.interp_data(ds)
			
			ds_list = Parallel(n_jobs=-1)(delayed(preprocess)(fn) for fn in self.file_list)
			ds_list = [x for x in ds_list if x is not None]
			if len(ds_list)==0:
				warnings.warn(f"读取数据为空")
				return None
			
			ds_merge = xr.merge(ds_list, compat='override')
			ds_merge = self.standard_data(ds_merge)
			
			print(f"数据加载成功！")
			return ds_merge
		
		elif self.file_type in ['txt']:
			def preprocess(fn):
				df = self.read_data(fn)
				if df is None:
					return None
				else:
					return self.standard_data(df)
			
			df_list = Parallel(n_jobs=-1)(delayed(preprocess)(fn) for fn in self.file_list)
			df_list = [x for x in df_list if x is not None]
			if len(df_list)==0:
				warnings.warn(f"读取数据为空")
				return None
			
			df_concat = pd.concat(df_list, axis=0).reset_index(drop=True)
			
			columns_new = ["name", "region", "lon", "lat", "period_id", "time"]
			columns_new = columns_new + [col for col in df_concat.columns if col not in columns_new]
			df_concat = df_concat[columns_new]
			
			print(f"数据加载成功！")
			return df_concat
		
		else:
			print("该文件类型读取功能暂未开发！")
			return None
	
	def load_data_region(self, region):
		
		print("———————— ————————")
		print(f"Loading -> {region}")
		
		if self.file_type in ['nc', 'netcdf']:
			def preprocess(fn):
				ds = self.read_data(fn)
				if ds is None:
					return None
				else:
					ds = self.interp_data(ds)
				try:
					ds = select_region(ds, region, col='PAC').groupby("region").mean()
					return ds
				except Exception as e:
					print(e)
					return None
			
			ds_list = Parallel(n_jobs=-1)(delayed(preprocess)(fn) for fn in self.file_list)
			# ds_list = [preprocess(fn) for fn in self.file_list]
			ds_list = [x for x in ds_list if x is not None]
			
			if len(ds_list)==0:
				warnings.warn(f"读取数据为空")
				return None
			
			ds_merge = xr.merge(ds_list, compat='override')
			ds_merge = self.standard_data(ds_merge)
			
			print(f"数据加载成功！")
			
			return ds_merge
		else:
			print("该文件类型读取功能暂未开发！")
			return None
	
	def load_data_points(self, points: np.ndarray):
		
		print("———————— ————————")
		print(f"Loading -> {points}")
		
		if self.file_type in ['nc', 'netcdf']:
			def preprocess(fn):
				ds = self.read_data(fn)
				if ds is None:
					return None
				else:
					ds = self.interp_data(ds)
				try:
					ds = select_points(ds, points)
				except Exception as e:
					print(e)
				return ds
			
			ds_list = Parallel(n_jobs=-1)(delayed(preprocess)(fn) for fn in self.file_list)
			ds_list = [x for x in ds_list if x is not None]
			
			if len(ds_list)==0:
				warnings.warn(f"读取数据为空")
				return None
			
			ds_merge = xr.merge(ds_list, compat='override')
			ds_merge = self.standard_data(ds_merge)
			
			print(f"数据加载成功！")
			
			return ds_merge
		else:
			print("该文件类型读取功能暂未开发！")
			return None


if __name__=='__main__':
	# 执行时间
	time_exec = datetime.datetime(2023, 10, 19)
	
	reader = Reader('1km', dtlist=None, varlist=['tp'])  # data = reader.load_data()

