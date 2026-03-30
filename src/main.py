
import os
from datetime import datetime, timedelta

from streamflow_percentiles.functions import (
    activate_usgs_api_key,
    get_usgs_gage_metadata, 
    get_flow_data_time_series, 
    get_recent_values
    )
from streamflow_percentiles.map_fxns import (
    create_gage_condition_map, 
    prep_for_plotting, 
    add_map_title
    )
from streamflow_percentiles.percentile_fxns import ( 
    get_rolling_avg_flow_data, 
    get_percentiles, 
    interpolate_percentile_of_recent_values, 
    get_years_used_for_percentile_calcs
    ) 

path_maps = r'content\html-maps'

def main():
    print("Hello from streamflow-percentiles!")
    today_str = str(datetime.today().date())
    yesterday_str = str(datetime.today().date() - timedelta(days=1))
    print(today_str)
    
    activate_usgs_api_key()
    sites = get_usgs_gage_metadata()
    sites = sites.iloc[-3:]
    sites_id = sites.monitoring_location_id 
    flow_data = get_flow_data_time_series(sites_id, today_str)
        
    for day in [1, 7, 14, 28]:
        print(f"Calculating {day}-day Percentiles")
        flow_data_nday = get_rolling_avg_flow_data(flow_data, day)
        recent_dvs = get_recent_values(flow_data_nday, yesterday_str)

        percentiles = get_percentiles(flow_data_nday, today_str)
        percentile_year_count = get_years_used_for_percentile_calcs(percentiles)
        df = interpolate_percentile_of_recent_values(recent_dvs, percentiles)

        df_gage = prep_for_plotting(df, sites, percentile_year_count)
        m = create_gage_condition_map(df_gage, 'daily', 'value', 'NWD', 'Current Daily Mean')
        add_map_title(f'{day}-Day Streamflow Percentiles {yesterday_str}', m)
        m.save(os.path.join(path_maps, f'streamflow-percentiles-{day:02d}day.html'))        

if __name__ == "__main__":
    main()
