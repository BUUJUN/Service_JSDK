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
		"time_fmt": "%Y%m%d",
		"type": "nc",
		"vars": {
			'tp': 'PRE', 'sp': 'PRS',
			't2m': 'TMP', 'd2m': 'DPT', 'r2m': 'RH',
			'ws10': 'WIN', 'u10': 'WIU', 'v10': 'WIV'
		},
	},
	"5km": {
		"prefix": os.path.join(dsfile_root, "4-5km_grid"),
		"format": "5KM_{var}_{ymdh}.nc",
		"time_re": r"5KM_\w+_(\d+).nc",
		"time_fmt":"%Y%m%d%H",
		"type": "nc",
		"vars": {
			'tp': 'PRE', 'sp': 'PRS',
			't2m': 'TMP', 'd2m': 'DPT', 'r2m': 'RH',
			'ws10': 'WIN', 'u10': 'WIU', 'v10': 'WIV'
		},
	},
	"skjc": {
		"prefix": os.path.join(dsfile_root, "9-skjc"),
		"format": os.path.join("**", "SURF-{region}-{period}-{time_str}.txt"),
		"time_re": r"SURF-\w+-\w+-(\d+).txt",
		"time_fmt": None,
		"type": "txt",
		"vars": {
			'tp': 'PRE_1h', 'sp': 'PRS',
			't2m': 'TEM', 'r2m': 'RHU',
			'ws10': 'WIN_S_Avg_2mi',
			'wd10': 'WIN_D_Avg_2mi',
			'tcc':'CLO_Cov'
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
			"yq": pd.Period(dt, freq='Q-DEC').strftime("%Y0%q") ,
			"yw": dt.strftime('%Y%W'),
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
			"yq": '*',
			"yw": '*',
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
	
	if 'wd10' in varlist:
		varlist.extend(['u10', 'v10'])
		varlist.remove('wd10')
	
	varlist = [file_vars[v] if v in file_vars else v for v in varlist]
	
	varlist = list(set(varlist))
	
	# print(varlist)
	
	fns = list()
	
	for dt in dtlist:
		kwargs = convert_time(dt)
		kwargs.update(fmt_kwargs)
		
		if dsname in ['skjc']:
			period_time = {
				"DAY": kwargs['ymd'],
				"HOUR": kwargs['ymdh'],
				"MON": kwargs['ym'],
				"QUARTER": kwargs['yq'],
				"WEEK": kwargs['yw'],
			}
			
			if not 'period' in fmt_kwargs.keys():
				periods = ['DAY']
			
			else:
				if fmt_kwargs['period'] is None or fmt_kwargs['period'] == '*':
					periods = ['DAY', 'MON', 'QUARTER']
				
				elif type(fmt_kwargs['period']) is str:
					periods = [fmt_kwargs['period']]
				
				else:
					periods = fmt_kwargs['period']
			
			periods = list( set(periods) & set(period_time) )
			
			if not periods:
				print(f"输入参数`period`有误，应为 {set(period_time)}")
				return []
			
			time_strs = [period_time[p] for p in periods]
			
			for t, p in zip(time_strs, periods):
				kwargs.update({'period':p, 'time_str':t})
				fn_format = file_fmt.format(**kwargs)
				fns.extend(glob(os.path.join(file_path, fn_format)))
		
		else:
			for v in varlist:
				fn_format = file_fmt.format(var=v, **kwargs)
				# print("[File Format]:\t", fn_format)
				# print(os.path.join(file_path, fn_format))
				fns.extend(glob(os.path.join(file_path, fn_format)))
	
	fns = sorted(list(set(fns)))
	
	return fns

def extract_times(dsname, filenames):
	
	if not filenames:
		return dict()
	
	time_re = file_info[dsname]['time_re']
	time_fmt = file_info[dsname]['time_fmt']
	
	times = {}
	
	for fn in filenames:
		time_str = re.findall(time_re, fn)[0]
		
		try:
			if time_fmt is None:
				if  "hour" in fn.lower():
					times.update({fn: pd.to_datetime(time_str, format="%Y%m%d%H")})
				elif "day" in fn.lower():
					times.update({fn: pd.to_datetime(time_str, format="%Y%m%d")})
				elif "week" in fn.lower():
					times.update({fn: pd.to_datetime(time_str+"-1", format="%Y%U-%w").to_period(freq='W')})
				elif "mon" in fn.lower():
					times.update({fn: pd.to_datetime(time_str, format="%Y%m")})
				elif "quarter" in fn.lower():
					times.update({fn: pd.Period(time_str[:4]+"Q"+time_str[-1], freq="Q")})
				elif "year" in fn.lower():
					times.update({fn: pd.to_datetime(time_str, format="%Y")})
				else:
					times.update({fn: pd.to_datetime(time_str)})
			else:
				times.update({fn: pd.to_datetime(time_str, format=time_fmt)})
		
		except Exception as e:
			print(f"Can't convert string: '{time_str}' to datetime.", e)
			times.update({fn: time_str})
			
	return times
