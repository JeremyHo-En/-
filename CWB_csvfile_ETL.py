#%%
import copy
import glob
import multiprocessing
import os
import pickle
import sys
import time
import datetime
import joblib
from obspy.core import UTCDateTime
import numpy as np
from scipy.io import loadmat
import pandas as pd

f_mseed = glob.glob(os.path.join('/Users/jeremy/Desktop/2023project/cwb_csv/*csv'))
f_mseed.sort()

stationlist = ['466920','467490','467410','466900','466910','466940','466990','467080','467420','467480','467490','467530','467540','467550','467571','467590','467610','467650','467660']

newarray = []
index_number = 0

def process_datetime(datetime_str):
    HOUR = datetime_str[-2:]
    try:
        if HOUR == '24':
            utc_time = UTCDateTime(datetime_str[:8]) + datetime.timedelta(days=1)
        else:
            utc_time = UTCDateTime(datetime_str)
        return utc_time.strftime('%Y-%m-%d %H:00:00')
    except Exception as e:
        print(f"Date:{datetime_str},error:{e}")
        return None

def read_csv_with_flexible_header(path):
    try:
        return pd.read_csv(path, header=74)
    except:
        return pd.read_csv(path, header=77)

def process_file(path):
    global index_number, newarray
    print(f"file:{path}")
    df = read_csv_with_flexible_header(path)
    for datatime in df[df['stno'].astype(str) == '466920']['yyyymmddhh']:
        processed_time = process_datetime(str(datatime))
        if not processed_time:
            continue
        year_series = pd.Series([processed_time], name='localTime_TW')
        station_data = []
        for sta in stationlist:
            try:
                station_df = df[(df['yyyymmddhh'] == datatime) & (df['stno'].astype(str) == sta)][['PS01','PP01','TX01','RH01','WD01','WD02']]
                station_df.columns = [f'PS01_{sta}', f'PP01_{sta}', f'TX01_{sta}',f'RH01_{sta}', f'WD01_{sta}', f'WD02_{sta}']
                station_df.reset_index(drop=True, inplace=True)
                station_data.append(station_df)
            except Exception as e:
                print(f"Station  {sta} Fail , error:{e}")
                station_data.append(pd.DataFrame([[np.nan, np.nan, np.nan]], columns=[f'PS01_{sta}', f'PP01_{sta}', f'TX01_{sta}',f'RH01_{sta}', f'WD01_{sta}', f'WD02_{sta}']))
        temparray = pd.concat([year_series] + station_data, axis=1)
        temparray.index = [str(index_number)]
        newarray = pd.concat([newarray, temparray]) if len(newarray) else temparray
        index_number += 1
        print(f"Loop:{index_number} Finish")

for path in f_mseed:
    process_file(path)

newarray.to_csv('/Users/jeremy/Desktop/CWB_weather_199912to202312.csv', index=False)
print("Data processing is complete and has been saved.")
# %%
