import os

import numpy as np
import pandas as pd

from typing import List
from .models import FeedPath

class Feed:
    def __init__(self, feed_path: dict) -> None:
        self.feed_path = FeedPath.parse_obj(feed_path)
        self.stops = self.read_parquet(self.feed_path.stops)
        self.stop_times = self.read_parquet(self.feed_path.stop_times)
        self.trips = self.read_parquet(self.feed_path.trips)
        self.transfers = self.read_parquet(self.feed_path.transfers)
        self.calendar = self.read_parquet(self.feed_path.calendar)
        self.weekday: List[str] = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', "saturday", "sunday"]
        self.trips_calendar = self.get_trips_calendar()

    def get_ndarray(self, df: pd.DataFrame) -> np.ndarray:
        ndarray = np.empty(len(df), dtype=[(k, v) for k, v in df.dtypes.to_dict().items()])
        for col in df.columns:
            ndarray[col] = df[col].to_list()
        return ndarray

    def read_parquet(self, path: str) -> np.ndarray:
        df = pd.read_parquet(path)
        return self.get_ndarray(df)
    
    def get_trips_calendar(self):
        _trips = pd.read_parquet(self.feed_path.trips)[["trip_id", "service_id"]]
        _calendar = pd.read_parquet(self.feed_path.calendar)
        df = pd.merge(_trips, _calendar, on="service_id")
        return self.get_ndarray(df)
    
    def get_available_trips(self, isoweekday: int) -> list:
        return self.trips_calendar[self.trips_calendar[self.weekday[isoweekday - 1]] == 1]["trip_id"].tolist()
    
    def get_stop_ids_from_parent_station(self, parent_station: str):
        return self.stops[self.stops["parent_station"] == parent_station]["stop_id"].tolist()

def load_data_from_path() -> Feed:
    feed_path = {
        "stops": os.environ.get("STOPS_PATH") ,
        "trips": os.environ.get("TRIPS_PATH"),
        "stop_times": os.environ.get("STOP_TIMES_PATH"),
        "transfers": os.environ.get("TRANSFERS_PATH"),
        "calendar": os.environ.get("CALENDAR_PATH"),
    }

    return Feed(feed_path)