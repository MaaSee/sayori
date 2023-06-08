#%%
import os
import time

from .raptor import search_point_to_point

from .models import FeedPath, Feed

def load_data_from_path() -> FeedPath:
    feed_path = {
        "stops": os.environ.get("STOPS_PATH") ,
        "trips": os.environ.get("TRIPS_PATH"),
        "stop_times": os.environ.get("STOP_TIMES_PATH"),
        "transfers": os.environ.get("TRANSFERS_PATH"),
        "calendar": os.environ.get("CALENDAR_PATH"),
    }

    return FeedPath.parse_obj(feed_path)

def main() -> None:
    # Read feed path
    feed_path = load_data_from_path()
    # Create feed
    feed = Feed.from_feed_path(feed_path)

    #define input data
    req = {
        "origin_stop_id": "0120",
        "destination_stop_id": "0150",
        "input_date": "2023-05-31",
        "input_secs": 5 * 60 * 60,
        "transfers_limit": 1,
    }

    # req = {
    #     "origin_stop_ids": "371900",
    #     "destination_stop_ids": "371050",
    #     "input_date": "2023-05-31",
    #     "input_secs": 11 * 60 * 60,
    #     "transfers_limit": 1,
    # }

    req = {
        "origin_stop_ids": ["399150-01"],
        "destination_stop_ids":  ["399093-01"],
        "input_date": "2023-05-31",
        "input_secs": 11 * 60 * 60,
        "transfers_limit": 0,
    }
    tic = time.perf_counter()
    res = search_point_to_point(feed, req)
    toc = time.perf_counter()

    print(f"elapsed time of point to point raptor search: {toc - tic} sec." )
    print(res)

if __name__ == "__main__":
    main()




#%%
