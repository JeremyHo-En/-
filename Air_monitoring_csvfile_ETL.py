#%%
import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, time
from datetime import timedelta
# %%
def detect_encoding(file_path):
    encodings = ['utf-8', 'big5']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read()
            return encoding
        except UnicodeDecodeError:
            continue
    raise Exception("Unable to open the file using any encoding method.")

def makecsv_for_single_year(path):
    air_list = ['CO', 'NO', 'NO2', 'NOx', 'O3', 'PM10', 'PM2.5', 'SO2']
    year = os.path.basename(path)[0:4]
    date_range = pd.date_range(start=f"{year}-01-01", end=f"{year}-01-01", freq="D")
    df = pd.read_csv(path,encoding=detect_encoding(path))
    if int(year) <= 2017:
        datename = df.columns[0]
    else :
        datename = df.columns[1]
    airname = df.columns[2]
    timename = df.columns[3:27].astype(str).tolist()
    for time_date in date_range:
        mask_time = pd.Timestamp(time_date.date())
        df[datename] = df[datename].apply(pd.Timestamp)
        mask1=(df[datename] == mask_time)
        mask2=df[airname].astype(str).isin(air_list)
        temp = df[mask1&mask2]
        nparray_temp = temp[timename].to_numpy().T
        missing_columns = set(air_list) - set(temp[airname])
        if len(missing_columns) > 0:
            print(f"Missing columns:{missing_columns}")
            misstemp = []
            for missing_v in missing_columns:
                misstemp.append(int(air_list.index(missing_v)))
            for col in misstemp:
                nparray_temp = np.insert(nparray_temp, col, np.nan, axis=1)
            missing_columns = set(air_list) - set(temp[airname])
            if np.shape(nparray_temp)[1] == 8:
                print('Value imputation successful')
        new = pd.DataFrame(nparray_temp,columns=air_list)
        new = new.replace(r'[^0-9.]', np.nan, regex=True)
        new = new.apply(pd.to_numeric, errors='coerce')
        new = new.fillna(method='ffill')
        new = new.fillna(method='bfill')
        new = new.astype(float)
        stime = datetime.combine(time_date.date(), time(hour=int(timename[0])))
        if str(timename[-1]) == '24':
            etime = time_date.date()+ timedelta(days=1)
        else :
            etime = datetime.combine(time_date.date(), time(hour=int(timename[-1])))
        date_tag = pd.date_range(start=stime, end=etime, freq="H")
        new.insert(0, 'Datetime', date_tag)
        print(time_date.date())
    date_range = pd.date_range(start=f"{year}-01-02", end=f"{year}-12-31", freq="D")
    for time_date in date_range:
        mask_time = pd.Timestamp(time_date.date())
        df[datename] = df[datename].apply(pd.Timestamp)
        mask1=(df[datename] == mask_time)
        if not mask1.any():
            print(f'{mask_time} Nofind')
            continue
        mask2=df[airname].astype(str).isin(air_list)
        temp = df[mask1&mask2]
        missing_columns = set(air_list) - set(temp[airname])
        nparray_temp = temp[timename].to_numpy().T
        if len(missing_columns) > 0:
            print(f"Missing columns:{missing_columns}")
            misstemp = []
            for missing_v in missing_columns:
                misstemp.append(int(air_list.index(missing_v)))
            for col in misstemp:
                nparray_temp = np.insert(nparray_temp, col, np.nan, axis=1)
            if np.shape(nparray_temp)[1] == 8:
                print('Value imputation successful')
        temp_new = pd.DataFrame(nparray_temp,columns=air_list)
        temp_new = temp_new.replace(r'[^0-9.]', np.nan, regex=True)
        temp_new = temp_new.apply(pd.to_numeric, errors='coerce')
        temp_new = temp_new.fillna(method='ffill')
        temp_new = temp_new.fillna(method='bfill')
        temp_new = temp_new.astype(float)
        stime = datetime.combine(time_date.date(), time(hour=int(timename[0])))
        if str(timename[-1]) == '24':
            etime = time_date.date()+ timedelta(days=1)
        else :
            etime = datetime.combine(time_date.date(), time(hour=int(timename[-1])))
        date_tag = pd.date_range(start=stime, end=etime, freq="H")
        temp_new.insert(0, 'Datetime', date_tag)

        new = pd.concat([new, temp_new], axis=0, ignore_index=True)
        print(time_date.date())
    return new

def makecsv_for_single_station(paths,station_name):
    air_list = ['CO', 'NO', 'NO2', 'NOx', 'O3', 'PM10', 'PM2.5', 'SO2']
    new = makecsv_for_single_year(paths[0])
    for path in paths[1:]:
        print(station_name)
        temp_new = makecsv_for_single_year(path)
        new = pd.concat([new, temp_new], axis=0, ignore_index=True)
    new.columns = ['datetime']+[col + f"_{station_name}" for col in air_list]

    return new

def makecsv_for_all_station(stationlist):
    for station_name in stationlist:
        print(station_name)
        f_mseed = glob.glob(os.path.join(f'/Users/jeremy/Desktop/CVDs/Air_raw/{station_name}/*csv'))
        f_mseed.sort()
        if station_name == stationlist[0]:
            new = makecsv_for_single_station(f_mseed,station_name)
        else :
            temp_new = makecsv_for_single_station(f_mseed,station_name)
            new = pd.merge(new, temp_new, on='datetime', how='inner')
    
    return new

#%%
station_namelist = ['Songshan','Qianzhe','ChungMing']
test_all = makecsv_for_all_station(station_namelist)
df_unique = test_all.drop_duplicates(subset='datetime', keep='first')
df_unique.to_csv('air_monitoring.csv', index=False, encoding='utf-8')
# %%
