# -*- coding: utf-8 -*-
"""
Created on 2024/08/30 13:25

@author: Bojun Wang
"""
import numpy as np

T0 = 243.15

lat_interp = np.arange(15.03, 60.03, 1)
lon_interp = np.arange(70.03, 140.03, 1)
# lat_interp = np.arange(15.03, 60.03, 0.01)
# lon_interp = np.arange(70.03, 140.03, 0.01)

var_renames = {
	"1km": {
		"lon": 'lon', "lat": 'lat',
		"PRE": 'tp', "PRS": 'sp',
		"TEM": 't2m', "DPT": 'd2m', "RH": 'r2m',
		"WIN": 'ws10', "WIU": 'u10', "WIV": 'v10',
	},
	"5km": {
		"lon": 'lon', "lat": 'lat',
		"PRE": 'tp', "PRS": 'sp',
		"TEM": 't2m', "DPT": 'd2m', "RH": 'r2m',
		"WIN": 'ws10', "WIU": 'u10', "WIV": 'v10',
	},
	"skjc":{
		"lon": 'lon', "lat": 'lat',
		"PRE_1h": 'tp', "PRS": 'sp',
		"TEM": 't2m', "RHU": 'r2m',
		"WIN_S_Avg_2mi": 'ws10', "WIN_D_Avg_2mi": 'wd10',
		"CLO_Cov": 'tcc',
	}
}

var_reattrs = {
	'lon': {'units': 'degrees_east', 'long_name': 'longitude', 'standard_name': 'longitude'},
	'lat': {'units': 'degrees_north', 'long_name': 'latitude', 'standard_name': 'latitude'},
	'tp': {'units': 'mm/day', 'long_name': 'Total precipitation', 'standard_name': 'total_precipitation'},
	'cp': {'units': 'mm/day', 'long_name': 'Convective precipitation', 'standard_name': 'convective_precipitation'},
	'lsp': {'units': 'mm/day', 'long_name': 'Large-scale precipitation', 'standard_name': 'large_scale_precipitation'},
	'sp': {'units': 'hPa', 'long_name': 'Surface pressure', 'standard_name': 'surface_pressure'},
	'ssrd': {'units': 'J/m2', 'long_name': 'Surface solar radiation downwards', 'standard_name': 'surface_solar_radiation_downwards'},
	'issrd': {'units': 'W/m2', 'long_name': 'Instantaneous surface solar radiation downwards', 'standard_name': 'instantaneous_surface_solar_radiation_downwards'},
	't2m': {'units': 'C_degrees', 'long_name': '2 metre temperature', 'standard_name': '2m_temperature'},
	'd2m': {'units': 'C_degrees', 'long_name': '2 metre dewpoint temperature', 'standard_name': '2m_dewpoint_temperature'},
	'r2m': {'units': '%', 'long_name': '2 metre relative humidity', 'standard_name': '2m_relative_humidity'},
	'u10': {'units': 'm/s', 'long_name': '10 metre U wind component', 'standard_name': '10m_u_component_of_wind'},
	'v10': {'units': 'm/s', 'long_name': '10 metre V wind component', 'standard_name': '10m_v_component_of_wind'},
	'ws10': {'units': 'm/s', 'long_name': '10 metre wind speed', 'standard_name': '10m_wind_speed'},
	'wd10': {'units': 'degree', 'long_name': '10 metre wind direction', 'standard_name': '10m_wind_direction'},
}

