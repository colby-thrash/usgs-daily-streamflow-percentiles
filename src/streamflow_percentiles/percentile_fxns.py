from datetime import datetime, timedelta
import numpy as np
import hyswap
import pandas as pd
from .helper_fxns import qaqc_usgs_data


def get_percentile_levels():
    '''
    Define what percentile levels (thresholds) to use for calculations
    Intervals of 5 or less are recommended to have sufficient levels to interpolate between in later calculations.
    Note that 0 and 100 percentile levels are ignored, refer to min and max values returned instead.
    
    Output:
        np.array of percentile levels for future percentile calculations 
    '''
    percentile_levels = np.concatenate((np.array([1]), np.arange(5,96,5), np.array([99])), axis=0)
    return percentile_levels


def get_rolling_avg_flow_data(flow_data: dict[str, pd.DataFrame], day: int, col: str='value') -> dict[str, pd.DataFrame]:
    '''
    Input: 
        Dict of dfs with time sries of daily streamflow data
        dfs are output from fxn get_flow_data_time_series() using the USGS daily data API

    Output:
        Dict of dfs of the same shape as the input with 'day'-day rolling averages applied
    '''
    
    flow_data_nday = {}
    for site_id, df in flow_data.items():
        df = qaqc_usgs_data(df, col)
        if not df.empty:
            flow_data_nday[site_id] = hyswap.utils.rolling_average(df, col, f'{day}D').round(2)
    return flow_data_nday


def calculate_single_day_percentile_thresholds(df, today, percentile_levels, col='value') -> pd.DataFrame:
    '''
    Calculate data values for percentiles defined by percentile_levels
    Tutorial did calculations for every day of the year, this code only calculates for the single date of interest

    Input: 
        df: time series of all data for single gage
        today: date as str for last data update
        percentile_levels: np.array of percentile values from 0-100 
        col: column of df for percentile calculation

    Output:
        df of percentiles with date (mm-dd) as index and percentile_level as columns
    '''
    
    yesterday_md = str(datetime.strptime(today, '%Y-%m-%d').date() - timedelta(1))[-5:]
    s_doy = hyswap.filter_data_by_month_day(df, yesterday_md, col)
    ptile_thresholds = hyswap.calculate_fixed_percentile_thresholds(s_doy, percentiles=percentile_levels)

    return ptile_thresholds


def get_percentiles(flow_data, today, col='value', filt_col='approval_status') -> dict[str, pd.DataFrame]:
    '''
    Calculate percentiles of n-day rolling averaged time series data.

    Input: 
        flow_data = dict of dfs with time series of n-day rolling averaged 
        today: date string 'YYYY-MM-DD' for day of calculation
        col = column of df to do percentile calculations
        filt_col = column of df to use as filter for approved data

    Output: Dict
        key: site_id
        value: pd.DataFrame of percentiles for date `today`
    '''

    percentile_thresholds = {}

    for site_id in flow_data:
    
        if flow_data[site_id].empty:
            print(f'{site_id} does not have daily data')
            continue
            
        ## Commented out to get every gage with data into the percentile_thresholds output. 
        ## < 30 yrs is now handled in interpolate_percentile_of_recent_values()
        # if (flow_data[site_id].index[-1] - flow_data[site_id].index[0]) / timedelta(days=365) < 30: 
        #     print(f'{site_id} does not have 30 years of data')
        #     continue
        
        if col in flow_data[site_id].columns:
            # print(f'{site_id} precentile calculated')
            # Filter data as only approved data in NWIS should be used to calculate statistics
            df = hyswap.utils.filter_approved_data(flow_data[site_id], filt_col)

            # Method to only calculate for the current day of year of interest
            percentile_thresholds[site_id] = calculate_single_day_percentile_thresholds(df, today, get_percentile_levels())        
            
        else:
            print('No standard discharge data column found for site ' + site_id + ', skipping')

    return percentile_thresholds


def interpolate_percentile_of_recent_values(
        recent_dvs: pd.DataFrame, 
        percentile_values, 
        col='value', 
        grp_col='monitoring_location_id'
    ) -> pd.DataFrame:

    '''
    Uses percentile values calculated from the entire dataset to interpolate the estimated percentile value for a new value

    Input: 
        recent_dvs: df with single value for each 
        percentile_values: 
        col = column of df to do percentile calculations

    Output: 
        returns recent_dvs with null data removed and the addition of column 'est_pct'
    '''

    df = pd.DataFrame()

    for site_id, site_df in recent_dvs.groupby(grp_col):
        if site_id not in list(percentile_values.keys()):
            # print(f'{site_id} not in percentile_values')
            continue
        elif percentile_values[site_id].isnull().all().all():
            # print(f'{site_id} all values Null')
            continue
        
        if site_df[col].isna().values[-1]:
            # print(f'{site_id} recent_dvs is Nan')
            continue 
        
        if percentile_values[site_id]['count'].values[0] < 30: # if less than 30 yrs of data
            percentile = np.nan
        else:
            percentile = hyswap.percentiles.calculate_fixed_percentile_from_value(
                site_df[col], percentile_values[site_id])
    
        site_df['est_pct'] = percentile                
        df = pd.concat([df, site_df])
    
    return df


def get_years_used_for_percentile_calcs(percentile_values) -> dict[str, int]:
    '''
    Determine how many years of data were used for percentile calculation

    Input: 
        percentile_values: dict of dfs with percentile data. df is from hyswap.calculate_single_day_percentile_thresholds() called in get_percentiles()

    Output: Dict
        key: site_id
        value: int of years used in percentile calculating
    '''

    percentile_year_count = {}
    for site_id in percentile_values:
        percentile_year_count[site_id] = percentile_values[site_id]['count'].values[0]
    return percentile_year_count
