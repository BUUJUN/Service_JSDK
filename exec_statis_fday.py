# -*- coding: utf-8 -*-
"""
Created on 2024/09/04 09:10

@author: Bojun Wang
"""
import datetime
import warnings
import pandas as pd
from core import statis_from_day, convert_time

warnings.filterwarnings('ignore')


if __name__=='__main__':
	# time_now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
	# time_exec = convert_time(time_now)
	time_exec = pd.date_range(start='2023-01-01T00', end='2023-12-31T23', freq='h')
	print(f"统计时间段: {time_exec[0]} ... {time_exec[-1]}")
	
	df_region_his = pd.read_excel("statics_region.xlsx")
	df_points_his = pd.read_excel("statics_points.xlsx")
	
	# 根据当前时间计算上个月范围
	time_end_mon = time_exec[0] - pd.DateOffset(days=time_exec[0].day)
	time_begin_mon = time_end_mon + pd.DateOffset(months=-1, days=1)
	print(time_begin_mon, time_end_mon)
	df_region_mon = statis_from_day(df_region_his, time_begin_mon, time_end_mon, periods=['month'])
	df_points_mon = statis_from_day(df_region_his, time_begin_mon, time_end_mon, periods=['month'])
	
	# 根据当前时间计算上个季度范围
	delta_mon = (time_exec[0].month - 1) % 3
	time_end_quat = time_exec[0] - pd.DateOffset(months=delta_mon, days=time_exec[0].day)
	time_begin_quat = time_end_quat + pd.DateOffset(months=-3, days=1)
	print(time_begin_quat, time_end_quat)
	df_region_quat = statis_from_day(df_region_his, time_begin_quat, time_end_quat, periods=['quarter'])
	df_points_quat = statis_from_day(df_region_his, time_begin_quat, time_end_quat, periods=['quarter'])
	
	# 根据当前时间计算上个年范围
	time_end_year = pd.Timestamp(year=time_exec[0].year, month=1, day=1) - pd.DateOffset(days=1)
	time_begin_year = time_end_mon + pd.DateOffset(years=-1, days=1)
	print(time_begin_year, time_end_year)
	df_region_year = statis_from_day(df_region_his, time_begin_year, time_end_year, periods=['year'])
	df_points_year = statis_from_day(df_region_his, time_begin_year, time_end_year, periods=['year'])
	
	dfs_region = [df_region_his, df_region_mon, df_region_quat, df_region_year]
	dfs_region = [x for x in dfs_region if x is not None]
	dfs_points = [df_points_his, df_points_mon, df_points_quat, df_points_year]
	dfs_points = [x for x in dfs_points if x is not None]
	
	df_region_concat = pd.concat(dfs_region, axis=0)
	df_points_concat = pd.concat(dfs_points, axis=0)
	
	with pd.ExcelWriter('statics_region_from_day.xlsx', engine='openpyxl') as writer:
		df_region_concat.to_excel(writer, index=False)
	
	with pd.ExcelWriter('statics_region_from_day.xlsx', engine='openpyxl') as writer:
		df_points_concat.to_excel(writer, index=False)
		
