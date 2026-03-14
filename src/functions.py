
import os
import glob
from datetime import datetime, timedelta
from dataretrieval import nwis
import pandas as pd
from .helper_fxns import qaqc_usgs_data
import hyswap

# print(os.getcwd())
path_data = r'data\daily'

with open('usgs_api_key.txt', 'r') as f:
    api_key = f.readline()
    os.environ["API_USGS_PAT"] = api_key
    

def get_usgs_gage_metadata(today):
    
    state = 'MO'
    # Query NWIS for what streamgage sites were active within the last week
    sites, _ = nwis.what_sites(
        stateCd=state, 
        parameterCd=['00060'], 
        # period="P1W"
        startDt=f'{today[:4]}-01-01'
    ) #, siteType='ST')
    
    return sites


def get_usgs_daily_api(site_no, start='1850-01-01', end=None, pcode='00060'):
        
    df = nwis.get_record(
            sites=site_no, 
            parameterCd=pcode, 
            start=start, 
            end=end, 
            service='dv'
        ).drop('site_no', axis=1) # don't need to save the extra data if it's in the file name. Can add back later if needed. 

    return df


def load_local_data(fn):
    return pd.read_csv(fn, index_col='datetime', parse_dates=['datetime'])

def update_local_data(fn_local, fn_today, site_no, today):
    '''
    Update local data if gage data already exists and adding recent data to it. 
        Loads local data as df
        Downloads only recent data from USGS API
        Combine dfs
        Delete old fn
        Write data to new fn
        Return df 
    '''
    
    df_local = load_local_data(fn_local)
    
    if df_local.empty: # this shouldn't happen again in the future bc of checks in get_flow_data_time_series
        return df_local
    
    # last_local_date = fn_site.split('_')[-1].strip('.csv')
    last_local_date = df_local.index[-1].replace(tzinfo=None)
    
    if last_local_date < datetime.strptime(today, '%Y-%m-%d'):
        
        query_start_date_str = str((last_local_date + timedelta(days=1)).date())
        df_new = get_usgs_daily_api(site_no, start=query_start_date_str)     ## add end = yesterday to get rid of potential date mixup       

        df = pd.concat([df_local, df_new])
        os.remove(fn_local)
        df.to_csv(fn_today)

    else: 
        df = df_local
        
    return df



def get_flow_data_time_series(sites: pd.DataFrame, today) -> dict[str, pd.DataFrame]:
    '''
    sites: df is output of nwis.what_sites()

    output: dictionary keys is string site number with df time series of all daily data for the site

    first run will download all data from USGS API and save locally. 
    subsequent runs will check for local data and  request only new data from USGS API if needed. 
    '''

    flow_data = {}
    
    # path_data = r'..\data\daily'
    
    for site_no in sites['site_no'].iloc[:]:
    
        fn_today = os.path.join(path_data, site_no + f'_{today}.csv')
        glob_site = glob.glob(os.path.join(path_data, site_no + '*csv'))

        print(fn_today, end=' - ')
    
        ## Check if gage & today's date exist
        if os.path.isfile(fn_today):
            print('1')
            df = load_local_data(fn_today)
            # flow_data[site_no] = df
            # continue
            
        ## Check if gage but not today's date exist. If so, only update data. Don't redownload the whole thing. 
        elif len(glob_site) > 0:
            print('2')
            fn_local = glob_site[-1]
            df = update_local_data(fn_local, fn_today, site_no, today)
            
        else:
            print('3')
            df = get_usgs_daily_api(site_no, end=today)
            if not df.empty:
                df.to_csv(fn_today)

        # flow_data[site_no] = qaqc_usgs_data(df, '00060_Mean') # breaks with an empty. WHen do I need to do qaqc?
        if not df.empty:
            flow_data[site_no] = df

        
    return flow_data



#%% Hack to load data without making a call to nwis.what_sites to get the gages

def get_sites_local():
    path_data = r'..\data\daily'
    sites_lst = []
    for fn in os.listdir(path_data):
        if '.csv' in fn:
            sites_lst.append(fn.split('_')[0])
    return sites_lst


def get_recent_values(flow_data, today, day=1) -> pd.DataFrame:
    '''
    Get last n days worth of data to use as "current". Get rolling average

    today = str of day data were collected. So typically last day of data should be from "yesterday"
    
    day = 1, 7, 14, or 24

    returns df with single, {day}-day averaged value for each gage. 
    '''

    assert day in [1, 7, 14, 28], 'Only for 1, 7, 14, or 28 day averages'    

    
    stop = yesterday = datetime.strptime(today, '%Y-%m-%d') - timedelta(1)
    start = yesterday
    if day > 1:
        start = yesterday - timedelta(day-1) #-1 bc both ends are inclusive in date index

    recent_dvs = pd.DataFrame()
    for site_no, df in flow_data.items():

        df = qaqc_usgs_data(df, '00060_Mean')
            
        if not df.empty:  
            df['site_no'] = site_no
            df_rolled = hyswap.utils.rolling_average(
                # df.iloc[-day:],   # initial method
                df.loc[str(start):str(yesterday)],  #use dates explicitly in case last date not consistent (this has happened) 
                '00060_Mean', 
                f'{day}D').dropna()  
            recent_dvs = pd.concat([recent_dvs, df_rolled], axis=0)
    
    recent_dvs.set_index('site_no', append=True, inplace=True) # adding it back into the df
    recent_dvs.swaplevel('site_no', 'datetime')  # trouble shooting formatting only
    
    return recent_dvs
