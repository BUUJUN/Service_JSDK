# -*- coding: utf-8 -*-
"""
Created on 2024/09/04 09:10

@author: Bojun Wang
"""
import datetime
import warnings
import pandas as pd

from configs import shp_prov, shp_city
from configs import points_cz
from core import Reader
from core import statis_region, statis_shp, statis_points
from core import convert_time

warnings.filterwarnings('ignore')



if __name__=='__main__':
	# time_now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
	# time_exec = convert_time(time_now)
	time_exec = pd.date_range(start='2023-01-01T00', end='2023-12-31T23', freq='h')
	print(f"统计时间段: {time_exec[0]} ... {time_exec[-1]}")
	
	reader_1km = Reader(dsname="5km", dtlist=time_exec.values, varlist=['t2m', 'tp', 'ws10', 'wd10', 'sp', 'r2m'])
	
	try:
		df_region = statis_region(reader=reader_1km, periods=["day"])
		with pd.ExcelWriter('statics_region.xlsx', engine='openpyxl') as writer:
			df_region.to_excel(writer, index=False)
	except Exception as e:
		print(e)
	
	try:
		df_region = statis_shp(reader_1km, periods=["day"], data_shps=[shp_prov, shp_city])
		with pd.ExcelWriter('statics_shp.xlsx', engine='openpyxl') as writer:
			df_region.to_excel(writer, index=False)
	except Exception as e:
		print(e)
	
	try:
		df_points = statis_points(reader=reader_1km, periods=["day"], points=points_cz)
		with pd.ExcelWriter('statics_points.xlsx', engine='openpyxl') as writer:
			df_points.to_excel(writer, index=False)
	except Exception as e:
		print(e)
	
	