from datetime import datetime, timedelta
import numpy as np
import hyswap
import pandas as pd


def get_percentil_levels():
    # Define what percentile levels (thresholds) that we want to calculate.
    # Intervals of 5 or less are recommended to have sufficient levels to interpolate between in later calculations.
    # Note that 0 and 100 percentile levels are ignored, refer to min and max values returned instead.
    percentile_levels = np.concatenate((np.array([1]), np.arange(5,96,5), np.array([99])), axis=0)
    return percentile_levels


def get_rolling_avg_flow_data(flow_data, day):
    flow_data_nday = {}
    for site_no, df in flow_data.items():
        if not df.empty:
            flow_data_nday[site_no] = hyswap.utils.rolling_average(df, '00060_Mean', f'{day}D').round(2)
    return flow_data_nday


## THIS IS THE FUNCTION THAT GREATLY REDUCES THE TIME IN THE SLOW PART OF THE CODE
def calculate_single_day_percentile_thresholds(df, today, percentile_levels) -> pd.DataFrame:
    '''
    input is [df time series of all data for single gage, value want percentile for, date as str for last data update]

    Returns df of thresholds. The thresholds (percentile_levels) and percentile values are tied together.
    '''
    
    # yesterday_md = str(datetime.today().date() - timedelta(days=1))[-5:]
    yesterday_md = str(datetime.strptime(today, '%Y-%m-%d').date() - timedelta(1))[-5:]
    
    s_doy = hyswap.filter_data_by_month_day(df, yesterday_md, '00060_Mean')
    # display(s_doy)
    
    ptile_thresholds = hyswap.calculate_fixed_percentile_thresholds(s_doy, percentiles=percentile_levels)
    # ptile_thresholds.index = list(yesterday_md)
    # display(ptile_thresholds)            
    
    # ptile = hyswap.calculate_fixed_percentile_from_value(value, ptile_thresholds)
    # print(f"{value}: {ptile}%, n={ptile_thresholds["count"].values}")

    return ptile_thresholds


## calculate percentiles of n day rolling avreaged time series data   

def get_percentiles(sites, flow_data, today) -> dict[str, pd.DataFrame]:
    '''
    flow_data = dict[site_no: df.nday]

    output = Dict
          key: site_no
          value: pd.DataFrame of percentiles for single day
    '''

    percentile_thresholds = {}
    # for site_no in sites['site_no']:
    for site_no in flow_data:
    
        if flow_data[site_no].empty:
            print(f'{site_no} does not have daily data')
            continue
            
        if (flow_data[site_no].index[-1] - flow_data[site_no].index[0]) / timedelta(days=365) < 30: 
            print(f'{site_no} does not have 30 years of data')
            continue
        
        if '00060_Mean' in flow_data[site_no].columns:
            print(f'{site_no} precentile calculated')
            # Filter data as only approved data in NWIS should be used to calculate statistics
            df = hyswap.utils.filter_approved_data(flow_data[site_no], '00060_Mean_cd')

            ## New method to only calculate for the current day of year of interest
            percentile_thresholds[site_no] = calculate_single_day_percentile_thresholds(df, today, get_percentil_levels())        
            
        else:
            print('No standard discharge data column found for site ' + site_no + ', skipping')

    return percentile_thresholds



## Use percentile thresholds to interpolate the percentile value for the new value. 

def interpolate_percentile_of_recent_values(recent_dvs: pd.DataFrame, percentile_values):

    df = pd.DataFrame()
    for site_no, site_df in recent_dvs.groupby(level="site_no", group_keys=True):  
        # print(f'{site_no} {len(site_df)}')
        if site_no not in list(percentile_values.keys()):
            # print(f'{site_no} not in percentile_values')
            ## TODO: need to add this somehow into the dataframe so I can plot the gages with less than 30 yrs of data?
            continue
        elif percentile_values[site_no].isnull().all().all():
            # print(f'{site_no} all values Null')
            continue
        # percentiles = hyswap.percentiles.calculate_multiple_variable_percentiles_from_values(
        #     site_df,'00060_Mean', percentile_values[site_no])
        
        if site_df['00060_Mean'].isna().values[-1]:
            # print(f'{site_no} past_dvs_nd is Nan')
            continue 
            
        percentile = hyswap.percentiles.calculate_fixed_percentile_from_value(
            site_df['00060_Mean'], percentile_values[site_no])
    
        site_df['est_pct'] = percentile                
        df = pd.concat([df, site_df])
    
    return df


def get_years_used_for_percentile_calcs(percentile_values) -> dict[str, int]:
    percentile_year_count = {}
    for site_no in percentile_values.keys():
        percentile_year_count[site_no] = percentile_values[site_no]['count'].max()
    return percentile_year_count