import datetime
from typing import List, Optional
import pydantic
import numpy as np
import pandas as pd

import pandera as pa
from pandera.typing import Series, DataFrame

class Stops(pa.SchemaModel):   
    stop_id: Series[str] = pa.Field(unique = True, nullable=False)
    stop_name: Series[str] = pa.Field(nullable=True)
    parent_station: Series[str] = pa.Field(nullable=True)
    platform_code: Series[str] = pa.Field(nullable=True)
    stop_lat: Series[pa.Float64] = pa.Field(nullable=True)
    stop_lon: Series[pa.Float64] = pa.Field(nullable=True)

    class Config:
        strict = True

class Trips(pa.SchemaModel):
    trip_id: Series[str] = pa.Field(unique = True, nullable=True)
    route_id: Series[str] = pa.Field(nullable=False)
    service_id: Series[str] = pa.Field(nullable=False)
    trip_headsign: Series[str] = pa.Field(nullable=True)
    trip_short_name: Series[str] = pa.Field(nullable=True)
    block_id: Series[str] = pa.Field(nullable=True)

    class Config:
        strict = True

class StopTimes(pa.SchemaModel):
    trip_id: Series[str] = pa.Field(nullable=False)
    stop_sequence: Series[pa.Int64] = pa.Field(nullable=False)
    stop_id: Series[str] = pa.Field(nullable=False)
    arrival_time: Series[pa.Int32] = pa.Field(nullable=True)
    departure_time: Series[pa.Int32] = pa.Field(nullable=True)
    pickup_type: Series[pa.Int32] = pa.Field(nullable = True, isin = [0, 1, 2, 3])
    drop_off_type: Series[pa.Int32] = pa.Field(nullable = True, isin = [0, 1, 2, 3])

    class Config:
        strict = True

class Calendar(pa.SchemaModel):
    calendar_date: Series[pa.Date] = pa.Field(unique = True, nullable=False) 
    service_ids: Series[pa.Object] = pa.Field(nullable=False) 

    class Config:
        strict = True

class Transfers(pa.SchemaModel):
    from_stop_id: Series[str] = pa.Field(nullable = False)
    to_stop_id: Series[str] = pa.Field(nullable = False)
    transfer_type: Series[pa.Int32] = pa.Field(nullable = False, isin = [0, 1, 2, 3])
    min_transfer_time: Series[pa.Int32] = pa.Field(nullable = False, gt = 0)

class TimeToStop(pydantic.BaseModel):
    time_to_reach: int = 0
    routing_path: List[str] = pydantic.Field(default_factory=list)
    routing_path_optional: np.ndarray = pydantic.Field(default_factory=lambda: np.empty(0, dtype=[("trip_id", "object"), ("stop_sequence", "int64"), ("stop_id", "object")]))
    preceding: List[Optional[str]] = pydantic.Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

class FeedPath(pydantic.BaseModel):
    stops: str
    stop_times: str
    trips: str
    transfers: str
    calendar: str

class RequestParameter(pydantic.BaseModel):
    origin_stop_ids: List[str]
    destination_stop_ids: List[str]
    specified_date: str
    specified_secs: int
    transfers_limit: int
    is_reverse_search: bool = False
    available_trip_ids: Optional[List[str]] = None

class RequestParameterIsochrones(pydantic.BaseModel):
    origin_stop_ids: List[str]
    # destination_stop_ids: List[str]
    specified_date: str
    specified_secs: int
    transfers_limit: int
    is_reverse_search: bool = False
    available_trip_ids: Optional[List[str]] = None


class Feed(pydantic.BaseModel):
    stops: np.ndarray
    stop_times: np.ndarray
    trips: np.ndarray
    transfers: np.ndarray
    calendar: np.ndarray
    
    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def from_pandas(
        cls, 
        stops: DataFrame[Stops], 
        stop_times: DataFrame[StopTimes], 
        trips: DataFrame[Trips], 
        transfers: DataFrame[Transfers], 
        calendar: DataFrame[Calendar]
    ) -> "Feed":

        stops = Stops.validate(stops)
        stop_times = StopTimes.validate(stop_times)
        trips = Trips.validate(trips)
        transfers = Transfers.validate(transfers)
        calendar = Calendar.validate(calendar)

        return cls.parse_obj({
            "stops": cls.convert_pandas2ndarray(stops),
            "stop_times": cls.convert_pandas2ndarray(stop_times),
            "trips": cls.convert_pandas2ndarray(trips),
            "transfers": cls.convert_pandas2ndarray(transfers),
            "calendar": cls.convert_pandas2ndarray(calendar),
        })

    @classmethod
    def from_feed_path(cls, feed_path: FeedPath) -> "Feed":
        stops = pd.read_parquet(feed_path.stops)
        stop_times = pd.read_parquet(feed_path.stop_times)
        trips = pd.read_parquet(feed_path.trips)
        transfers = pd.read_parquet(feed_path.transfers)
        calendar = pd.read_parquet(feed_path.calendar)

        stops = Stops.validate(stops)
        stop_times = StopTimes.validate(stop_times)
        trips = Trips.validate(trips)
        transfers = Transfers.validate(transfers)
        calendar = Calendar.validate(calendar)

        return cls.parse_obj({
            "stops": cls.convert_pandas2ndarray(stops),
            "stop_times": cls.convert_pandas2ndarray(stop_times),
            "trips": cls.convert_pandas2ndarray(trips),
            "transfers": cls.convert_pandas2ndarray(transfers),
            "calendar": cls.convert_pandas2ndarray(calendar),
        })

    
    @staticmethod
    def convert_pandas2ndarray(df: pd.DataFrame) -> np.ndarray:
        ndarray = np.empty(len(df), dtype=[(k, v) for k, v in df.dtypes.to_dict().items()])
        for col in df.columns:
            ndarray[col] = df[col].to_list()
        return ndarray

    def get_available_trips(self, date: datetime.date) -> list:        
        return (
            self.trips[
                np.isin(
                    self.trips["service_id"], 
                    self.calendar[self.calendar["calendar_date"] == date]["service_ids"][0]
                )
            ]["trip_id"].tolist()
        )

    def get_stop_ids_from_parent_station(self, parent_station: str) -> list:
        return self.stops[self.stops["parent_station"] == parent_station]["stop_id"].tolist()