
# %%
import datetime
import time
import numpy as np

from typing import List, Dict, Optional, Union
from .models import TimeToStop, RequestParameter, Feed

class StopAccessStates:
    def __init__(self, from_stop_ids: List[str], specified_date: str, specified_secs: int) -> None:
        self.from_stop_ids: List[str] = from_stop_ids
        self.specified_date: datetime.date = datetime.date.fromisoformat(specified_date)
        self.specified_secs: int = specified_secs
        self.time_to_stops: Dict[str, TimeToStop] = {origin_stop_id: TimeToStop() for origin_stop_id in from_stop_ids}
        self.already_processed_xfers: List[str] = []
        self.just_updated_stops: List[str] = from_stop_ids.copy()

    def get_all_stops(self) -> List[str]:
        return list(self.time_to_stops.keys())
    
    def get_preceding(self, stop_id: str) -> List[str]:
        return self.time_to_stops[stop_id].preceding

    def get_time_to_reach(self, stop_id: str) -> int:
        return self.time_to_stops[stop_id].time_to_reach

    def get_routing_path(self, stop_id: str) -> List[str]:
        return self.time_to_stops[stop_id].routing_path
    
    def get_routing_path_optional(self, stop_id: str) -> np.array:
        return self.time_to_stops[stop_id].routing_path_optional
    
    def get_last_trip_id(self, stop_id:str) -> Optional[str]:
        if len(self.get_preceding(stop_id)) > 0:
            return self.get_preceding(stop_id)[-1] 
        else:
            return None
    
    def time_to_reach_to_destinations(self, destination_stop_ids: List[str]):
        return [
            time_to_stop.dict() | ({"stop_id": stop_id}) \
                for stop_id, time_to_stop in self.time_to_stops.items() \
                    if stop_id in destination_stop_ids
        ]
        
    def create_time_to_reach(self, stop_id: str, time_to_reach: int) -> None:
        self.time_to_stops[stop_id] = TimeToStop(time_to_reach=time_to_reach)
        return None

    def update_time_to_reach(self, stop_id: str, time_to_reach: int) -> None:
        self.time_to_stops[stop_id].time_to_reach = time_to_reach
        return None

    def update_preceding_path(self, stop_id: str, preceding_path: List[str]) -> None:
        self.time_to_stops[stop_id].preceding = preceding_path
        return None

    def update_path(self, stop_id: str, routing_path: List[str]) -> None:
        self.time_to_stops[stop_id].routing_path = routing_path
        return None

    def update_path_optional(self, stop_id: str, routing_path_optional: np.array) -> None:
        self.time_to_stops[stop_id].routing_path_optional = routing_path_optional
        return None
    
    def update_stop_access_state(
        self,
        stop_id: str,
        time_to_reach: Union[int, float],
        routing_path: List[str],
        routing_path_optional: np.array,
        trip_id: Optional[str] = None,
        preceding_path: Optional[List[str]] = None,
    ) -> bool:
        # initialize return object
        did_update = False
        if stop_id in self.get_all_stops():
            if self.get_time_to_reach(stop_id) > time_to_reach:
                # update the stop access attributes
                self.update_time_to_reach(stop_id, time_to_reach)
                did_update = True
        else:
            self.create_time_to_reach(stop_id, time_to_reach)
            did_update = True

        if did_update:
            self.update_path(stop_id, routing_path)
            self.update_path_optional(stop_id, routing_path_optional)
            # override if a preceding path is provided
            if preceding_path:
                self.update_preceding_path(stop_id, preceding_path)
            # add current trip id to the path of trips taken, avoiding dupes
            if trip_id is not None \
                and (len(self.get_preceding(stop_id)) == 0 or trip_id != self.get_preceding(stop_id)[-1]):
                self.time_to_stops[stop_id].preceding.append(trip_id)
            
        return did_update


def stop_times_for_kth_trip(
    stop_state: StopAccessStates,
    feed: Feed,
    is_reverse_search: bool,
    available_trip_ids: Optional[List[str]]
) -> None:
    # get service_id 
    available_trips = feed.get_available_trips(stop_state.specified_date)
    trip_stop_pairings = {}
    for ref_stop_id in stop_state.just_updated_stops:
        # find all trips already related to this stop
        associated_trips: List[str] = stop_state.get_preceding(ref_stop_id)
        # find all qualifying trips assocaited with this stop
        if is_reverse_search:
            related_trips = feed.stop_times[(feed.stop_times["stop_id"] == ref_stop_id) * (feed.stop_times["arrival_time"] <= stop_state.specified_secs)]["trip_id"]
        else:
            related_trips = feed.stop_times[(feed.stop_times["stop_id"] == ref_stop_id) * (feed.stop_times["departure_time"] >= stop_state.specified_secs)]["trip_id"]
        # find potential trips intersecting available or usable trip_ids
        if isinstance(available_trip_ids, list):
            potential_trips = set(related_trips) & set(available_trip_ids)
        else:
            potential_trips = set(related_trips) & set(available_trips)
        
        for potential_trip in potential_trips:
            # pass on trips that are already addressed
            if potential_trip in associated_trips:
                continue
                
            if potential_trip in trip_stop_pairings.keys():
                trip_stop_pairings[potential_trip].append(ref_stop_id)
            else:
                trip_stop_pairings[potential_trip] = [ref_stop_id]

    # iterate through trips with grouped stops in them
    for trip_id in trip_stop_pairings:
        stop_ids = trip_stop_pairings[trip_id]

        # get all the stop time arrivals for that trip
        stop_times_sub = feed.stop_times[(feed.stop_times["trip_id"] == trip_id)]
        stop_times_sub = stop_times_sub[stop_times_sub["stop_sequence"].argsort()]

        # find all stop ids that are in this stop ordering and pick last on route path
        target_stops = stop_times_sub[np.isin(stop_times_sub["stop_id"], stop_ids)]
        if is_reverse_search:
            target_stops = target_stops[target_stops["stop_sequence"].argsort()[::-1]]    
        else:
            target_stops = target_stops[target_stops["stop_sequence"].argsort()]

        # get the "hop on" point
        from_here = target_stops[-1]       
        ref_stop_id = from_here["stop_id"]
        # are we continuing from some previous path of trips?
        preceding_path = stop_state.get_preceding(ref_stop_id)
        # how long it took to get to the stop so far (0 for start node)
        baseline_cost = stop_state.get_time_to_reach(ref_stop_id)
        # get all following stops
        if is_reverse_search:
            stop_times_after = stop_times_sub[stop_times_sub["stop_sequence"] <= from_here["stop_sequence"]]
        else:
            stop_times_after = stop_times_sub[stop_times_sub["stop_sequence"] >= from_here["stop_sequence"]]

        # for all following stops, calculate time to reach
        for departure_time, arrive_time, arrive_stop_id, arrive_stop_sequence in zip(stop_times_after["departure_time"], stop_times_after["arrival_time"], stop_times_after["stop_id"], stop_times_after["stop_sequence"]):
            # time to reach is diff from start time to arrival (plus any baseline cost)
            if is_reverse_search:
                arrive_time_adjusted = stop_state.specified_secs - departure_time + baseline_cost
                # get current routing path and combine preceding path
                current_routing_path = stop_times_after[(stop_times_after["stop_sequence"] >= arrive_stop_sequence)]["stop_id"].tolist()    
                current_routing_path_optional = stop_times_after[(stop_times_after["stop_sequence"] >= arrive_stop_sequence)][["trip_id", "stop_sequence", "stop_id"]]
            else:
                arrive_time_adjusted = arrive_time - stop_state.specified_secs + baseline_cost
                # get current routing path and combine preceding path
                current_routing_path = stop_times_after[(stop_times_after["stop_sequence"] <= arrive_stop_sequence)]["stop_id"].tolist()
                current_routing_path_optional = stop_times_after[(stop_times_after["stop_sequence"] <= arrive_stop_sequence)][["trip_id", "stop_sequence", "stop_id"]]
            
            # append current routing path to stopstate
            if len(stop_state.get_routing_path(ref_stop_id)) == 0:
                routing_path = stop_state.get_routing_path(ref_stop_id) + current_routing_path
                routing_path_optional = np.concatenate([stop_state.get_routing_path_optional(ref_stop_id), current_routing_path_optional]) 
            else:
                if is_reverse_search:
                    routing_path = current_routing_path[:-1] + stop_state.get_routing_path(ref_stop_id)
                    routing_path_optional = np.concatenate([current_routing_path_optional[:-1], stop_state.get_routing_path_optional(ref_stop_id)])
                else:
                    routing_path = stop_state.get_routing_path(ref_stop_id) + current_routing_path[1:]
                    routing_path_optional = np.concatenate([stop_state.get_routing_path_optional(ref_stop_id), current_routing_path_optional[1:]])

            stop_state.update_stop_access_state(
                arrive_stop_id, 
                arrive_time_adjusted,
                routing_path,
                routing_path_optional,
                trip_id,
                preceding_path
            )

    return None

def add_footpath_transfers(
    stop_state: StopAccessStates,
    feed: Feed,
    is_reverse_search: bool
) -> List[str]:
    # initialize a return object
    updated_stop_ids = []
    # add in transfers to nearby stops
    stop_ids = stop_state.get_all_stops()
    for stop_id in stop_ids:
        # no need to re-intersect already done stops
        if stop_id in stop_state.already_processed_xfers:
            continue
        
        last_trip_id = stop_state.get_last_trip_id(stop_id)
        # only update if currently inaccessible or faster than currrent option
        for arrive_stop_id, transfers_cost in zip(feed.transfers[feed.transfers["from_stop_id"] == stop_id]["to_stop_id"], feed.transfers[feed.transfers["from_stop_id"] == stop_id]["min_transfer_time"]):
            # time to reach new nearby stops is the transfer cost plus arrival at last stop
            arrive_time_adjusted = stop_state.get_time_to_reach(stop_id)  + transfers_cost
            routing_path = [stop_id, arrive_stop_id]
            routing_path_optional = np.array(
                [("walk", 1, stop_id), ("walk", 2, arrive_stop_id)],
                dtype=[("trip_id", "object"), ("stop_sequence", "int64"), ("stop_id", "object")]
            )

            if len(stop_state.get_routing_path(stop_id)) == 0:
                routing_path = stop_state.get_routing_path(stop_id) + routing_path
                routing_path_optional = np.concatenate([stop_state.get_routing_path_optional(stop_id),routing_path_optional]) 
            else:
                if is_reverse_search:

                    routing_path = routing_path[:-1] + stop_state.get_routing_path(stop_id)
                    routing_path_optional = np.concatenate([routing_path_optional[:-1], stop_state.get_routing_path_optional(stop_id)]) 
                else:
                    routing_path = stop_state.get_routing_path(stop_id) + routing_path[1:]
                    routing_path_optional = np.concatenate([stop_state.get_routing_path_optional(stop_id), routing_path_optional[1:]]) 
                                                                                   
            did_update = stop_state.update_stop_access_state(
                arrive_stop_id,
                arrive_time_adjusted,
                routing_path,
                routing_path_optional,
                last_trip_id,
            )
            if did_update:
                updated_stop_ids.append(arrive_stop_id)
    
    return updated_stop_ids

def run_raptor(
    feed: Feed,
    from_stop_ids: List[str], 
    specified_date: str,
    specified_secs: int, 
    transfer_limit: int,
    is_reverse_search: bool,
    available_trip_ids: Optional[List[str]]
) -> StopAccessStates:
    # initialize lookup with start node taking 0 seconds to reach
    stop_state = StopAccessStates(from_stop_ids, specified_date, specified_secs)

    # setting transfer limit at 1
    for k in range (transfer_limit + 1):
        tic = time.perf_counter()
        stop_times_for_kth_trip(stop_state, feed, is_reverse_search, available_trip_ids)
        toc = time.perf_counter()

        # now add footpath transfers and update
        tic = time.perf_counter()
        just_updated_stops = add_footpath_transfers(stop_state, feed, is_reverse_search)
        toc = time.perf_counter()

        stop_state.already_processed_xfers += stop_state.just_updated_stops
        stop_state.just_updated_stops = just_updated_stops
    
    return stop_state


def search_p2p_geojson(feed: Feed, req: Dict[str, Optional[Union[str, int]]]) -> Optional[str]:
    # check input values
    request_paremeters = RequestParameter.parse_obj(req)
    # when is_reverse_search is true, reverse variables assignment of from and to stop_ids
    if request_paremeters.is_reverse_search:
        from_stop_ids = request_paremeters.destination_stop_ids
        to_stop_ids = request_paremeters.origin_stop_ids
    else:
        from_stop_ids = request_paremeters.origin_stop_ids
        to_stop_ids = request_paremeters.destination_stop_ids
    # set other variables
    specified_date = request_paremeters.specified_date
    specified_secs = request_paremeters.specified_secs
    transfers_limit = request_paremeters.transfers_limit
    is_reverse_search = request_paremeters.is_reverse_search
    available_trip_ids = request_paremeters.available_trip_ids

    # run raptor argolithum
    tic = time.perf_counter()
    stop_state = run_raptor(
        feed,
        from_stop_ids, 
        specified_date, 
        specified_secs, 
        transfers_limit,
        is_reverse_search,
        available_trip_ids
    )
    toc = time.perf_counter()

    # get duration from origin to destination 
    time_to_reach_to_destinations = stop_state.time_to_reach_to_destinations(to_stop_ids)
    # when route search is failed, return None 
    if len(time_to_reach_to_destinations) == 0:
        return None
    # find a shortest route seach result
    time_to_reach_to_destinations = sorted(time_to_reach_to_destinations, key=lambda x: x["time_to_reach"])
    fastest_way = time_to_reach_to_destinations[0]
    # form the result as a geojson format
    result = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [list(feed.stops[feed.stops["stop_id"] == stop_id][["stop_lon", "stop_lat"]].tolist()[0]) for stop_id in fastest_way["routing_path"]]
                },
                "properties": {k:int(v) if isinstance(v, (int, np.int64)) else v for k, v in fastest_way.items()}
            }
        ]
    }
    
    return result

def search_p2p_path(feed: Feed, req: Dict[str, Optional[Union[str, int]]]) -> Optional[str]:
    # check input values
    tic = time.perf_counter()
    request_paremeters = RequestParameter.parse_obj(req)
    # when is_reverse_search is true, reverse variables assignment of from and to stop_ids
    if request_paremeters.is_reverse_search:
        from_stop_ids = request_paremeters.destination_stop_ids
        to_stop_ids = request_paremeters.origin_stop_ids
    else:
        from_stop_ids = request_paremeters.origin_stop_ids
        to_stop_ids = request_paremeters.destination_stop_ids
    # set other variables
    specified_date = request_paremeters.specified_date
    specified_secs = request_paremeters.specified_secs
    transfers_limit = request_paremeters.transfers_limit
    is_reverse_search = request_paremeters.is_reverse_search
    available_trip_ids = request_paremeters.available_trip_ids
    toc = time.perf_counter()

    # run raptor argolithum
    # tic = time.perf_counter()
    stop_state = run_raptor(
        feed,
        from_stop_ids, 
        specified_date, 
        specified_secs, 
        transfers_limit,
        is_reverse_search,
        available_trip_ids
    )
    # toc = time.perf_counter()

    # get duration from origin to destination 
    time_to_reach_to_destinations = stop_state.time_to_reach_to_destinations(to_stop_ids)
    # when route search is failed, return None 
    if len(time_to_reach_to_destinations) == 0:
        return None
    # find a shortest route seach result
    time_to_reach_to_destinations = sorted(time_to_reach_to_destinations, key=lambda x: x["time_to_reach"])
    fastest_way = time_to_reach_to_destinations[0]
    # form the result as a geojson format
    fastest_way["routing_path_optional"] = [{"trip_id": row[0], "stop_sequence": int(row[1]), "stop_id": row[2]} for row in fastest_way["routing_path_optional"]]

    return {k:int(v) if isinstance(v, np.int64) else v for k, v in fastest_way.items() if k != "preceding"}

#%%

