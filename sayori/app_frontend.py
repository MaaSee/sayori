#%%
import os
import json

import pandas as pd
import geopandas as gpd
import streamlit as st
import leafmap.foliumap as leafmap
from streamlit_folium import st_folium

from .modules.loader import load_data_from_path
from .modules.raptor import search_point_to_point

#%%
@st.cache_data
def load_stops():
    stops = pd.read_parquet(os.environ.get("STOPS_PATH"))
    return stops

@st.cache_data
def load_feed():
    feed = load_data_from_path()
    return feed

stops = load_stops()
feed = load_feed()

with st.form("routing arguments", clear_on_submit=False):
    query_form = st.text_area(
        label="arguments of route search",
        value="""
        {
            "origin_stop_id": "371900",
            "destination_stop_id": "371050",
            "input_date": "2023-05-31",
            "input_secs": 39600,
            "transfers_limit": 0
        }
        """,
        height=300

    )
    submitted = st.form_submit_button("Seach")

    if submitted:
        query = json.loads(query_form)
        res = search_point_to_point(feed, query)


        r =json.loads(res)
        gdf = gpd.GeoDataFrame.from_features(r, crs="EPSG:4326")
        center = [gdf.centroid[0].coords[0][1], gdf.centroid[0].coords[0][0]]


        m = leafmap.Map(
            locate_control=True, 
            latlon_control=True, 
            draw_export=True, 
            minimap_control=True,
            location=center,
            zoom_start=14
        )
        m.add_geojson(r)
        st_data = st_folium(m, width=725)
