
import os
from datetime import datetime, timedelta
from src.functions import get_usgs_gage_metadata, get_flow_data_time_series, get_recent_values
from src.map_fxns import create_gage_condition_map, prep_for_plotting, add_map_title
from src.percentile_fxns import get_rolling_avg_flow_data, get_percentiles, interpolate_percentile_of_recent_values, get_years_used_for_percentile_calcs
from src.helper_fxns import remove_empty_df
from src.web_fxns import update_index_file


def main():
    print("Hello from streamflow-percentiles!")
    print(os.getcwd())
    today = str(datetime.today().date())
    yesterday_str = str(datetime.today().date() - timedelta(days=1))
    print(today)
    # today = '2026-03-09'

    sites = get_usgs_gage_metadata(today)
    sites = sites.iloc[:]
    flow_data = get_flow_data_time_series(sites, today)
    sites, flow_data = remove_empty_df(sites, flow_data)
        
    for day in [1, 7, 14, 28]:
        recent_dvs = get_recent_values(flow_data, today, day)
        flow_data_nday = get_rolling_avg_flow_data(flow_data, day)

        percentiles = get_percentiles(sites, flow_data_nday, today)
        percentile_year_count = get_years_used_for_percentile_calcs(percentiles)
        df = interpolate_percentile_of_recent_values(recent_dvs, percentiles)

        df_gage = prep_for_plotting(df, sites, percentile_year_count)
        m = create_gage_condition_map(df_gage, '00060_Mean', 'NWD', 'Current Daily Mean')
        add_map_title(f'{day}-Day Streamflow Percentiles {yesterday_str}', m)
        m.save(yesterday_str + f'_{day}day.html')        

    update_index_file(yesterday_str)

if __name__ == "__main__":
    main()
