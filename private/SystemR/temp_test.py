from matplotlib.pyplot import show, title
from sysdata.csvdata import csvFuturesData
from systems.provided.futures_chapter15.estimatedsystem import futures_system

data=csvFuturesData("private.SystemR.data")
system=futures_system(data=data)
system.config.forecast_weight_estimate["pool_instruments"]=True
system.config.forecast_weight_estimate["method"]="bootstrap" 
system.config.forecast_weight_estimate["equalise_means"]=False
system.config.forecast_weight_estimate["monte_runs"]=200
system.config.forecast_weight_estimate["bootstrap_length"]=104


system=futures_system(config=system.config)

system.combForecast.get_raw_forecast_weights("EDOLLAR").plot()
title("EDOLLAR")
show()
