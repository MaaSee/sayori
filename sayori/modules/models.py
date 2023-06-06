from typing import List, Optional
from pydantic import BaseModel, Field

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
    origin_stop_id: str
    destination_stop_id: str
    input_date: str
    input_secs: int
    transfers_limit: int
    is_reverse_search: bool = False
    available_trip_ids: Optional[List[str]] = None
