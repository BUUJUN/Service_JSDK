# -*- coding: utf-8 -*-
"""
Created on 2024/08/30 11:21

@author: Bojun Wang
____ ____
读取数据，并做标准化处理
"""
from typing import Union
import numpy as np
import datetime
import xarray as xr
from joblib import Parallel, delayed
import warnings

from configs import file_info, lat_interp, lon_interp, var_reattrs, var_renames, extract_files, extract_times, T0

class Reader(object):
	def __init__(self, dsname, dtlist: Union[list, np.ndarray] = None, varlist: list = None, **extract_kwargs):
		'''

		:param dsname:
		:param dtlist:
		:param extract_kwargs: vars:list
		'''
		self.dsname = dsname
		self.dtlist = dtlist
		self.varlist = varlist
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
		
	def read_data(self, filepath):
		print(f"Load -> {filepath}")
		if self.file_type in ['nc', 'netcdf']:
			try:
				ds = xr.open_dataset(filepath)
				ds = ds.rename({name:self.var_renames[name] for name in (set(self.var_renames)&set(ds.coords))})
				ds = ds.sortby('lon').sortby('lat')
				if not 'time' in ds.coords:
					ds = ds.expand_dims({'time':[self.time_dict[filepath]]})
				return ds
			except Exception as e:
				print(f"读取{filepath}错误", e)
				return None
	
	
	def interp_data(self, ds, **kwargs):
		lon_i = self.lon_interp
		lat_i = self.lat_interp
		
		print(
			f"Interp -> lon: ({lon_i[0]:.2f},{lon_i[-1]:.2f},{lon_i[1]-lon_i[0]:.2f})",
			f"lat: ({lat_i[0]:.2f},{lat_i[-1]:.2f},{lat_i[1]-lat_i[0]:.2f})")
		
		if ds is None: return None
		try:
			ds_interp = ds.interp(lon=lon_i, lat=lat_i, method='linear', **kwargs)
			return ds_interp
		except Exception as e:
			print(f"插值{list(ds.data_vars)}错误", e)
			return None
	
	
	def standard_data(self, ds):
		print("Standarding ...")
		# 重命名变量名
		ds_standard = ds.rename_vars({name:self.var_renames[name] for name in (set(self.var_renames)&set(ds.data_vars))})
		
		if self.dsname in ['1km']:
			if 'tp' in ds_standard.coords:
				ds_standard['tp'] = ds_standard.tp * 24
			if 't2m' in ds_standard.coords:
				ds_standard['t2m'] = ds_standard.t2m - T0
			if 'd2m' in ds_standard.coords:
				ds_standard['d2m'] = ds_standard.d2m - T0
			if 'sp' in ds_standard.coords:
				ds_standard['sp'] = ds_standard.sp * 0.01
			if 'u10' in ds_standard.coords and 'v10' in ds_standard.coords:
				u = ds_standard['u10']
				v = ds_standard['v10']
				ds_standard['wd10'] = (270 - np.arctan2(v, u) * 180 / np.pi) % 360
		
		[ds_standard[var].attrs.update(self.var_reattrs[var]) for var in set(self.var_reattrs) & set(ds_standard.variables)]
		
		ds_standard = ds_standard.sortby('time')
		
		dims_order = ['time','lon','lat'] + list(set(ds_standard.dims)-{'time','lon','lat'})
		ds_standard = ds_standard.transpose(*dims_order)
		
		if not self.varlist is None:
			ds_standard = ds_standard.drop_vars(list(set(ds_standard.data_vars)-set(self.varlist)))
		
		return ds_standard
	
	
	def load_data(self):
		def preprocess(fn):
			ds = self.read_data(fn)
			ds = self.interp_data(ds)
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


if __name__ == '__main__':
	# 执行时间
	time_exec = datetime.datetime(2023, 10, 19)
	
	reader = Reader('1km', dtlist=None, varlist=['tp'])
	data = reader.load_data()
	
