# -*- coding: utf-8 -*-
"""
Created on 2024/09/04 09:10

@author: Bojun Wang
"""
import pandas as pd
import numpy as np
import xarray as xr
from typing import Union
from joblib import Parallel, delayed

from configs import shp_prov, shp_city, period_id
from configs import points_cz
from core import Reader, select_shp, Statis_Period

import warnings
warnings.filterwarnings('ignore')

def statis_year(df, period_use="month"):
	
	print("计算年统计量 ... ")
	
	df_sel = df.loc[df.period_id==period_id[period_use]]
	
	if len(df_sel)==0:
		print("来源'{period}'的数据为空，将尝试其他 period ")
		
		for period_use in ["quarter", "month", "week", "day"]:
			print(f"尝试使用 {period_use} 的数据进行年统计")
			df_sel = df.loc[df.period_id==period_id[period_use]]
			if len(df_sel)!=0:
				print(f"成功获取到 {period_use} 的数据")
				break
	
	if len(df_sel)==0:
		print("统计失败！")
		return df
	
	df_sel["time"] = df_sel.time.str[:4]
	
	df_sel = df_sel.groupby(["time","region","name"]).mean()
	
	df_sel["period_id"] = period_id["year"]
	
	df_sel = df_sel.reset_index()
	
	df_concat = pd.concat([df, df_sel], axis=0).reset_index(drop=True)
	
	print("统计完成！")
	
	return df_concat


if __name__ == '__main__':
	
	reader = Reader(dsname="skjc", region="CITY", period="*", time="*")
	df_city = reader.load_data()
	
	reader = Reader(dsname="skjc", region="PROVINCE", period="*", time="*")
	df_prov = reader.load_data()
	
	df_regions = pd.concat([df_city, df_prov], axis=0)
	
	try:
		df_statis = statis_year(df_regions)
		with pd.ExcelWriter('statics_region_from_txt.xlsx', engine='openpyxl') as writer:
			df_statis.to_excel(writer, index=False)
	except Exception as e:
		print(e)