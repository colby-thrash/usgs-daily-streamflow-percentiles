
import os
import glob
from datetime import datetime, timedelta
from dataretrieval import waterdata, nwis
import pandas as pd
from .helper_fxns import qaqc_usgs_data, chunk_data
import hyswap

# print(os.getcwd())
path_data = r'data\daily'
if not os.path.isdir(path_data):
    os.makedirs(path_data)

def activate_usgs_api_key():
    # register for key: https://api.waterdata.usgs.gov/signup/
    try:
        with open(r'usgs_api_key.txt', 'r') as f:
            api_key = f.readline()
            os.environ["API_USGS_PAT"] = api_key
            print('USGS API key found successfully')
    except:
        print("Code being run without a USGS API key")

def get_usgs_gage_metadata_nwis(today):
    
    state = 'MO'
    # Query NWIS for what streamgage sites were active within this year
    sites, _ = nwis.what_sites(
        stateCd=state, 
        parameterCd=['00060'], 
        # period="P1W"
        startDt=f'{today[:4]}-01-01'
    ) #, siteType='ST')
    
    return sites

def get_usgs_gage_metadata(pcode='00060'):
    '''
    With the switch from nwis to waterdata, this is now a two step process. 
    The first call is to waterdata.get_time_series_metadata() 
    The location ids from that call are used as input to waterdata.get_monitoring_locations() which has more metadata about the site

    The output is a df similar to the sites df from the nwis implimentation but with some different properties and property names. 
    '''
    
    # Query Water Data APIs for what monitoring locations were active within the last week
    active_time_series, _ = waterdata.get_time_series_metadata(
        state_name='Missouri',
        parameter_code=pcode,
        statistic_id='00003', #daily mean
        end_utc='P1W',
        skip_geometry=True,
        )

    site_ids = active_time_series['monitoring_location_id'].unique().tolist()
    properties = [
        'monitoring_location_id', 
        'agency_code', 
        'monitoring_location_name', 
        'county_name',
        'drainage_area', 
        'site_type', 
        'hydrologic_unit_code', 
        'altitude', 
        'vertical_datum', 
        'original_horizontal_datum'
    ]
    
    active_stream_gages = pd.DataFrame()
    for chunk_ids in chunk_data(site_ids, 200):
        df, _ = waterdata.get_monitoring_locations(
            monitoring_location_id=chunk_ids,
            properties=properties,
            # site_type_code='ST',
        )
        
        active_stream_gages = pd.concat([active_stream_gages, df])
    
    return active_stream_gages

   
def get_usgs_daily_api(site_id, start='1850-01-01', pcode='00060'):
    
    properties = [
        'time', 
        'value', 
        'approval_status', 
        'qualifier',
        'monitoring_location_id',
        # 'parameter_code', 
        # 'statistic_id'
        # unit_of_measure
        ]
    
    df, _ = waterdata.get_daily(
        monitoring_location_id=site_id,
        parameter_code=pcode,
        statistic_id='00003', # mean daily discharge
        time = '/'.join([start, '..']),
        skip_geometry=True,
        properties = properties,
       )
    df.set_index('time', inplace=True)

    return df


def load_local_data(fn):
    return pd.read_csv(fn, parse_dates=['time']).set_index('time', drop=False)

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
    
    last_local_date = df_local.index[-1].replace(tzinfo=None)
    
    if last_local_date < datetime.strptime(today, '%Y-%m-%d'):
        
        query_start_date_str = str((last_local_date + timedelta(days=1)).date())
        df_new = get_usgs_daily_api('USGS-'+site_no, start=query_start_date_str)     ## add end = yesterday to get rid of potential date mixup?       

        df = pd.concat([df_local, df_new])
        os.remove(fn_local)
        df.to_csv(fn_today)

    else: 
        df = df_local
        
    return df



def get_flow_data_time_series(site_nos: iter, today: str) -> dict[str, pd.DataFrame]:
    '''
    site_nos: iterable of gage site numbers. Used in file name and as output dict keys. 

    output: dictionary keys is string site number with df time series of all daily data for the site

    first run will download all data from USGS API and save locally. 
    subsequent runs will check for local data and  request only new data from USGS API if needed. 
    '''

    flow_data = {}
        
    for site_no in site_nos:
    
        fn_today = os.path.join(path_data, site_no + f'_{today}.csv')
        glob_site = glob.glob(os.path.join(path_data, site_no + '*csv'))

        print(fn_today, end=' - ')
    
        ## Check if gage & today's date exist
        if os.path.isfile(fn_today):
            print('1')
            df = load_local_data(fn_today)
            # flow_data[site_no] = df
            # continue
            
        ## Check if find gage but not today's date. If so, only update data. Don't redownload the whole thing. 
        elif len(glob_site) > 0:
            print('2')
            fn_local = glob_site[-1]
            df = update_local_data(fn_local, fn_today, site_no, today)
            
        else:
            print('3')
            df = get_usgs_daily_api('USGS-'+site_no)
            if not df.empty:
                df.to_csv(fn_today)

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


def get_recent_values(flow_data, today, day=1, col='value') -> pd.DataFrame:
    '''
    Get last n days worth of data to use as "current". Get rolling average

    today = str of day data were collected. So typically last day of data should be from "yesterday"
    
    day = 1, 7, 14, or 24
    
    col = column of data frame to do data processing on.

    returns df with single, {day}-day averaged value for each gage. 
    '''

    stop = yesterday = datetime.strptime(today, '%Y-%m-%d') - timedelta(1)
    start = yesterday
    if day > 1:
        start = yesterday - timedelta(day-1) #-1 bc both ends are inclusive in date index

    recent_dvs = pd.DataFrame()
    for site_no, df in flow_data.items():

        df = qaqc_usgs_data(df, col)
        
        if not df.empty:  
            df['site_no'] = site_no
            df_rolled = hyswap.utils.rolling_average(
                # df.iloc[-day:],   # initial method
                df.loc[str(start):str(yesterday)],  #use dates explicitly in case last date not consistent (this has happened) 
                col, 
                f'{day}D').dropna(subset=col)  
            recent_dvs = pd.concat([recent_dvs, df_rolled], axis=0)
    
    return recent_dvs
