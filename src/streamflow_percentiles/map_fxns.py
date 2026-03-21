## Copied from tutorial:
# https://doi-usgs.github.io/hyswap/examples/site_conditions_examples.html

import folium
import geopandas as gpd
import pandas as pd
import hyswap

def prep_for_plotting(df, sites, percentile_year_count):
    # categorize streamflow by the estimated streamflow percentiles
    df = hyswap.utils.categorize_flows(df, 'est_pct', schema_name="NWD")
    
    # Prep Data for mapping by joining site information and flow data`
    gage_df = pd.merge(sites, df, how="right", on="monitoring_location_id")
    
    # Add year count to gage_df
    gage_df.set_index('site_no', inplace=True)
    gage_df['record_length_yr'] = pd.Series(percentile_year_count)
    gage_df.reset_index(inplace=True)

    return gage_df

def add_counties_to_map(m):
    path = r"C:\Users\nrthrac\Desktop\working\MWSS\python-gis\gis-data\MO_2014_County_Boundaries_shp"
    crs = 4326
    mo = gpd.read_file(path).to_crs(crs) #"EPSG:5070")

    alpha = 0.5
    wt = 1.5
    color = 'black'
    
    folium.GeoJson(mo.boundary.to_json(), 
              style_function=lambda x: {'weight': wt, 'color': color, 'opacity': alpha / 2}, 
              ).add_to(m)
    
    folium.GeoJson(mo.dissolve().boundary.to_json(), 
              style_function=lambda x: {'weight': wt, 'color': color, 'opacity': alpha / 2}, 
              ).add_to(m)

def add_map_title(title, m):
    title_html = '''
     <h3 align="center" style="font-size:24px"><b>{}</b></h3>
     '''.format(title)   

    m.get_root().html.add_child(folium.Element(title_html))


def create_gage_condition_map(gage_df, flow_data_type, flow_data_col, map_schema, streamflow_data_type):
        """
        Function to create a `gpd.explore()` map of site conditions.

        Parameters
        ----------
        gage_df : geopandas.GeoDataFrame
                A dataframe containing streamflow values and percentiles for all sites to be
                displayed in the map. Dataframe must contain the columns 'time', 'est_pct',
                'monitoring_location_id', and 'monitoring_location_name'
        flow_data_type : string
                One of "instantaneous" or "daily", describing the type of data in `gage_df`
        flow_data_col : string
                The name of the column containing the mapped flow data
        map_schema : string
                One of 'NWD', 'WaterWatch', 'WaterWatch_Drought', 'WaterWatch_Flood', 'WaterWatch_BrownBlue', and 'NIDIS_Drought'.
        streamflow_data_type : string
                A title to be added to the legend to describe the data displayed

        Returns
        -------
        df
                The input df with all rows where the data value is -999999 replaced with np.nan
        """
        # Format date and set to str type for use in map tooltips
        if flow_data_type == 'instantaneous':
                gage_df['Date'] = gage_df['time'].dt.strftime('%Y-%m-%d %H:%M')
        elif flow_data_type == 'daily':
                gage_df['Date'] = gage_df['time'].dt.strftime('%Y-%m-%d')
        gage_df = gage_df.drop('time', axis=1)
        # create colormap for map from hyswap schema
        schema = hyswap.utils.retrieve_schema(map_schema)
        flow_cond_cmap = schema['colors']
        if 'low_color' in schema:
                flow_cond_cmap = [schema['low_color']] + flow_cond_cmap
        if 'high_color' in schema:
                flow_cond_cmap = flow_cond_cmap + [schema['high_color']]
        # if creating a drought map, set handling of non-drought flows
        if map_schema in ['WaterWatch_Drought', 'NIDIS_Drought']:
                gage_df['flow_cat'] = gage_df['flow_cat'].cat.add_categories('Other')
                gage_df.loc[gage_df['flow_cat'].isnull(), 'flow_cat'] = 'Other'
                flow_cond_cmap = flow_cond_cmap + ['#e3e0ca'] # light taupe
        # set NA values to "Not Ranked" category
        gage_df['flow_cat'] = gage_df['flow_cat'].cat.add_categories('Not Ranked')
        gage_df.loc[gage_df['est_pct'].isna(), 'flow_cat'] = 'Not Ranked'
        flow_cond_cmap = flow_cond_cmap + ['#d3d3d3'] # light grey
        # renaming columns with user friendly names for map
        gage_df = gage_df.rename(columns={flow_data_col:'Discharge (cfs)',
                                                'est_pct':'Estimated Percentile',
                                                'monitoring_location_id':'USGS Gage ID',
                                                'monitoring_location_name':'Streamgage Name',
                                                'flow_cat':'Streamflow Category',
                                                'record_length_yr': 'Recond Length (yr)'
                                                })
        # Create map
        m = folium.Map(
                    location=(38.36768, -91.75),
                    zoom_start=7, 
                    tiles="Cartodb Positron",
        )
        
        add_counties_to_map(m)
        
        gage_df.set_crs(crs="EPSG:4326").to_crs("EPSG:5070").explore(
                                m=m,
                                column="Streamflow Category",
                                cmap=flow_cond_cmap,
                                tooltip=["USGS Gage ID", 
                                         "Streamgage Name", 
                                         "Streamflow Category", 
                                         "Discharge (cfs)", 
                                         "Estimated Percentile", 
                                         "Date", 
                                         "Recond Length (yr)"],
                                tiles="CartoDB Positron",
                                marker_kwds=dict(radius=5),
                                legend_kwds=dict(caption=streamflow_data_type + '<br> Streamflow  Category'))
        return m #returns a folium map object
    