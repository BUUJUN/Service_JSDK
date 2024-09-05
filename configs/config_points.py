# -*- coding: utf-8 -*-
"""
Created on 2024/09/04 17:13

@author: Bojun Wang
"""
import numpy as np

# 生成一些伪站点做测试
lon_test = np.arange(116, 119, 1)
lat_test = np.arange(31, 35, 1)
lon_grid, lat_grid = np.meshgrid(lon_test, lat_test)

points_cz = np.column_stack([lon_grid.ravel(), lat_grid.ravel()])