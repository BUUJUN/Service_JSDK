# -*- coding: utf-8 -*-
"""
Created on 2024/09/02 15:52

@author: Bojun Wang
"""
import os
from glob import glob
import pandas as pd
import re
import warnings

dsfile_root = "D:\statis_jiangsu\data"

file_info = {
	"root": os.path.join(dsfile_root),
	"1km": {
		"prefix": os.path.join(dsfile_root, "6-1km_grid"),
		"format": "1KM_{var}_{ymd}.nc",
		"time_re": r"1KM_\w+_(\d+).nc",
		"type": "nc",
		"vars": {
			'tp': 'PRE', 'sp': 'PRS',
			't2m': 'TMP', 'd2m': 'DPT', 'r2m': 'RH',
			'ws10': 'WIN', 'u10': 'WIU', 'v10': 'WIV'
		},
	}
}

def convert_time(dt):
	'''
	将 datetime 转为字符串, 用于根据 datetime 读取对应文件
	:param dt:
	:return:
	'''
	try:
		dt = pd.to_datetime(dt)
		return {
			"ym": dt.strftime("%Y%m"),
			"ymd": dt.strftime("%Y%m%d"),
			"ymdh": dt.strftime("%Y%m%d%H"),
			"ymdhm": dt.strftime("%Y%m%d%H%M"),
			"ymdhms": dt.strftime("%Y%m%d%H%M%S"),
			"ymdhm_fcst": dt.strftime("%Y%m%d%H%M"),
			"ymd_h_m_s_fcst": dt.strftime("%Y%m%d%H%M%S"),
			"hm": dt.strftime("%H%M"),
		}
	except Exception as e:
		# print(f"[Warning]:\t输入的变量 `dt={dt}` 的数据类型为 {type(dt)}")
		warnings.warn(str(e), Warning)
		return  {
			"ym": '*',
			"ymd": '*',
			"ymdh": '*',
			"ymdhm": '*',
			"ymdhms": '*',
			"ymdhm_fcst": '*',
			"ymd_h_m_s_fcst": '*',
			"hm": '*',
		}


def extract_files(dsname, dtlist: list = None, varlist: list = None, **fmt_kwargs):
	'''
	:param dsname:
	:param dtlist:
	:param varlist:
	:param fmt_kwargs:
	:return:
	'''
	file_path = file_info[dsname]['prefix']
	file_fmt = file_info[dsname]['format']
	file_vars = file_info[dsname]['vars']
	
	if dtlist is None:
		dtlist = ['*']
	
	if varlist is None:
		varlist = ['*']
	
	varlist = [file_vars[v] if v in file_vars else v for v in varlist]
	
	fns = list()
	
	for dt in dtlist:
		kwargs = convert_time(dt)
		kwargs.update(fmt_kwargs)
		
		for v in varlist:
			fn_format = file_fmt.format(var=v, **kwargs)
			print("[File Format]:\t", fn_format)
			fns.extend(glob(os.path.join(file_path, fn_format)))
	
	fns = sorted(list(set(fns)))
	
	return fns

def extract_times(dsname, filenames):
	time_fmt = file_info[dsname]['time_re']
	times = {f: pd.to_datetime(re.findall(time_fmt, f)[0]) for f in filenames}
	return times
