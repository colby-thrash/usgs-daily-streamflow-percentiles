## Copied from tutorial:
# https://doi-usgs.github.io/hyswap/examples/site_conditions_examples.html

import folium
import geopandas as gpd
import pandas as pd
import hyswap

def prep_for_plotting(df, sites, percentile_year_count):
    # categorize streamflow by the estimated streamflow percentiles
    df = hyswap.utils.categorize_flows(df, 'est_pct', schema_name="NWD")
    df = df.reset_index(level='datetime')
    # Prep Data for mapping by joining site information and flow data`
    gage_df = pd.merge(sites, df, how="right", on="site_no")
    
    # Add year count to gage_df
    gage_df.set_index('site_no', inplace=True)
    gage_df['record_length_yr'] = pd.Series(percentile_year_count)
    gage_df.reset_index(inplace=True)

    return gage_df

def add_counties_to_map(m):
    path = r"C:\Users\nrthrac\Desktop\working\MWSS\python-gis\gis-data\MO_2014_County_Boundaries_shp"
    crs = 4326
    mo = gpd.read_file(path,
                      ).to_crs(crs) #"EPSG:5070")

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


def create_gage_condition_map(gage_df, flow_data_col, map_schema, streamflow_data_type) -> folium.Map:
        # Format date and set to str type for use in map tooltips
        if flow_data_col == '00060':
                gage_df['Date'] = gage_df['datetime'].dt.strftime('%Y-%m-%d %H:%M')
        elif flow_data_col == '00060_Mean':
                gage_df['Date'] = gage_df['datetime'].dt.strftime('%Y-%m-%d')
        gage_df = gage_df.drop('datetime', axis=1)
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
    
        # gage_df = gage_df.rename(columns={flow_data_col:'Discharge (cfs)',
        #                                         'est_pct':'Estimated Percentile',
        #                                         'site_no':'USGS Gage ID',
        #                                         'station_nm':'Streamgage Name',
        #                                         'flow_cat':'Streamflow Category', 
        #                                         'record_length_yr': 'Recond Length (yr)'}
        #                         )

        gage_df = gage_df.rename(columns={flow_data_col:'Discharge (cfs)',
                                            'est_pct':'Estimated Percentile',
                                            'monitoring_location_id':'USGS Gage ID',
                                            'monitoring_location_name':'Streamgage Name',
                                            'flow_cat':'Streamflow Category', 
                                            'record_length_yr': 'Recond Length (yr)'}
                                )

        # convert dataframe to geopandas GeoDataFrame
        # gage_df = gpd.GeoDataFrame(gage_df,
        #                      geometry=gpd.points_from_xy(gage_df.dec_long_va,
        #                                                        gage_df.dec_lat_va),
        #                      crs="EPSG:4326").to_crs("EPSG:5070")

        gage_df = gage_df.set_crs(crs="EPSG:4326").to_crs("EPSG:5070")

        # Create map        
        m = folium.Map(
                    # location=(38.36768, -92.47729),  # orig
                    location=(38.36768, -91.75),
                    zoom_start=7, 
                    tiles="Cartodb Positron",
                    # tiles = "OpenStreetmap",
                    # tiles = "Stadia.Outdoors",
                    # tiles = "OpenTopoMap",
                    # tiles="CartoDB Voyager",
                    # tiles=None,
        )

        add_counties_to_map(m)
    
        gage_df.explore(m=m,
                            column="Streamflow Category",
                                cmap=flow_cond_cmap,
                                tooltip=["USGS Gage ID", "Streamgage Name", "Streamflow Category", "Discharge (cfs)", "Estimated Percentile", "Date", 'Recond Length (yr)'],
                                # tiles="CartoDB Positron",
                                marker_kwds=dict(radius=5),
                                legend_kwds=dict(caption=streamflow_data_type + '<br> Streamflow  Category', 
                                                 loc = 'upper right'))

        return m #returns a folium map object