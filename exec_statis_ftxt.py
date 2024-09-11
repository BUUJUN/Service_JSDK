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
from core import Reader, statis_from_txt, statis_from_day

import warnings
warnings.filterwarnings('ignore')

if __name__ == '__main__':
	# time_now = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
	# time_exec = convert_time(time_now)
	time_exec = pd.date_range(start='2023-01-01T00', end='2023-12-31T23', freq='h')
	print(f"统计时间段: {time_exec[0]} ... {time_exec[-1]}")
	
	df_txt_day = statis_from_txt(time_exec.values, periods='day')
	
	with pd.ExcelWriter('statics_region_from_txt.xlsx', engine='openpyxl') as writer:
		df_txt_day.to_excel(writer, index=False)