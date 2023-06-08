from typing import List, Optional
from pydantic import BaseModel, Field
import numpy as np
import pandas as pd

class TimeToStop(BaseModel):
    time_to_reach: int = 0
    routing_path: List[str] = Field(default_factory=list)
    preceding: List[Optional[str]] = Field(default_factory=list)


class FeedPath(BaseModel):
    stops: str
    stop_times: str
    trips: str
    transfers: str
    calendar: str

class RequestParameter(BaseModel):
    origin_stop_ids: List[str]
    destination_stop_ids: List[str]
    input_date: str
    input_secs: int
    transfers_limit: int
    is_reverse_search: bool = False
    available_trip_ids: Optional[List[str]] = None

class Feed(BaseModel):
    stops: np.ndarray
    stops: np.ndarray
    stop_times: np.ndarray
    trips: np.ndarray
    transfers: np.ndarray
    calendar: np.ndarray
    trips_calendar: np.ndarray
    weekday: List[str] = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', "saturday", "sunday"]
    
    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def from_pandas(
        cls, 
        stops: pd.DataFrame, 
        stop_times: pd.DataFrame, 
        trips: pd.DataFrame, 
        transfers: pd.DataFrame, 
        calendar: pd.DataFrame
    ) -> "Feed":
        
        trips_calendar = pd.merge(trips[["trip_id", "service_id"]], calendar, on="service_id")

        return cls.parse_obj({
            "stops": cls.convert_pandas2ndarray(stops),
            "stop_times": cls.convert_pandas2ndarray(stop_times),
            "trips": cls.convert_pandas2ndarray(trips),
            "transfers": cls.convert_pandas2ndarray(transfers),
            "calendar": cls.convert_pandas2ndarray(calendar),
            "trips_calendar": cls.convert_pandas2ndarray(trips_calendar)
        })

    @classmethod
    def from_feed_path(cls, feed_path: FeedPath) -> "Feed":
        stops = pd.read_parquet(feed_path.stops)
        stop_times = pd.read_parquet(feed_path.stop_times)
        trips = pd.read_parquet(feed_path.trips)
        transfers = pd.read_parquet(feed_path.transfers)
        calendar = pd.read_parquet(feed_path.calendar)

        trips_calendar = pd.merge(trips[["trip_id", "service_id"]], calendar, on="service_id")

        return cls.parse_obj({
            "stops": cls.convert_pandas2ndarray(stops),
            "stop_times": cls.convert_pandas2ndarray(stop_times),
            "trips": cls.convert_pandas2ndarray(trips),
            "transfers": cls.convert_pandas2ndarray(transfers),
            "calendar": cls.convert_pandas2ndarray(calendar),
            "trips_calendar": cls.convert_pandas2ndarray(trips_calendar)
        })

    
    @staticmethod
    def convert_pandas2ndarray(df: pd.DataFrame) -> np.ndarray:
        ndarray = np.empty(len(df), dtype=[(k, v) for k, v in df.dtypes.to_dict().items()])
        for col in df.columns:
            ndarray[col] = df[col].to_list()
        return ndarray

    def get_available_trips(self, isoweekday: int) -> list:
        return self.trips_calendar[self.trips_calendar[self.weekday[isoweekday - 1]] == 1]["trip_id"].tolist()
    
    def get_stop_ids_from_parent_station(self, parent_station: str) -> list:
        return self.stops[self.stops["parent_station"] == parent_station]["stop_id"].tolist()