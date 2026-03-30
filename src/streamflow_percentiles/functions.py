
import os
import sys
import glob
from datetime import datetime, timedelta
from dataretrieval import waterdata, nwis
import pandas as pd
import hyswap
from .helper_fxns import qaqc_usgs_data, chunk_data

path_data = r'data\daily'
if not os.path.isdir(path_data):
    os.makedirs(path_data)

def activate_usgs_api_key():
    # register for key: https://api.waterdata.usgs.gov/signup/
    try:
        # check for api key as input argument
        if len(sys.argv) > 1:
            print('FOUND ARGUMENTS!')
            api_key = sys.argv[1]

        else:
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

def get_usgs_gage_metadata(state_name='Missouri', pcode='00060'):
    '''
    With the switch from nwis to waterdata, this is now a two step process. 
    The first call is to waterdata.get_time_series_metadata() 
    The location ids from that call are used as input to waterdata.get_monitoring_locations() which has more metadata about the site

    Output: 
        df with metadata about the site. data columns are defined by properties list
        Note that column names are different from the NWIS API
    '''
    
    # Query Water Data APIs for what monitoring locations were active within the last week
    active_time_series, _ = waterdata.get_time_series_metadata(
        state_name=state_name,
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
        time='/'.join([start, '..']),
        skip_geometry=True,
        properties=properties,
       )
    df.set_index('time', inplace=True)

    return df


def load_local_data(fn):
    return pd.read_csv(fn, parse_dates=['time']).set_index('time')


def update_local_data(fn_local, fn_today, site_id, today):
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
        df_new = get_usgs_daily_api(site_id, start=query_start_date_str)     ## add end = yesterday to get rid of potential date mixup?       

        df = pd.concat([df_local, df_new])
        os.remove(fn_local)
        df.to_csv(fn_today)

    else: 
        df = df_local
        
    return df


def get_flow_data_time_series(site_ids, today) -> dict[str, pd.DataFrame]:
    '''
    Load dict of dfs with updated daily data time series.
    The first run will download all data from USGS API and save locally. 
    Subsequent runs will check for missing local data and will either
        1. request only new data from USGS API if needed 
        2. load local data with most recent data
    
    Input: 
        site_ids: iterable of strings of gage site ids. Used in file name and as output dict keys. 
        today: date as str for last data update
    
    Output: 
        Dict key: site number [str]
        Dict values: df time series of all daily data for the site
    '''

    flow_data = {}
    len_sites = len(site_ids)
        
    for i, site_id in enumerate(site_ids):
    
        fn_today = os.path.join(path_data, site_id + f'_{today}.csv')
        glob_site = glob.glob(os.path.join(path_data, site_id + '*csv'))

        print(fn_today, f'- {1+i:03}/{len_sites}', end=' - ')
    
        ## Check if gage & today's date exist locally
        if os.path.isfile(fn_today):
            print('Loading Local Data')
            df = load_local_data(fn_today)
            
        ## Check if find gage but not today's date. If so, only update data. Don't redownload the whole thing. 
        elif len(glob_site) > 0:
            print('Appending recent USGS API Data to Local Data')
            fn_local = glob_site[-1]
            df = update_local_data(fn_local, fn_today, site_id, today)
            
        else:
            print('Downloading all data from USGS API')
            df = get_usgs_daily_api(site_id)
            if not df.empty:
                df.to_csv(fn_today)

        if not df.empty:
            flow_data[site_id] = df
 
    return flow_data


def get_recent_values(flow_data, yesterday_str, col='value') -> pd.DataFrame:
    '''
    Get most recent time series value on or before yesterday and return as df. 

    Input: 
        flow_data: dict of n-day rolling averaged dfs with site_id str as keys
        yesterday_str = str of yesterdays data
        col = column of df to get recent values

    Output: 
        df with single n-day averaged value for each gage without NaN recent
    '''

    last_day_list = []
    for site_id, df in flow_data.items():
        if not df.empty:  
            last_day_list.append(df[:yesterday_str].iloc[-1])

    recent_dvs = pd.DataFrame(last_day_list).dropna(subset=col)
    recent_dvs.index.name = df.index.name  # explicitely assing index name bc it doesn't carry through pd.Series appending 
    
    return recent_dvs
