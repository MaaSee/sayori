#%%
from sayori.raptor import search_isochrones
from sayori.models import FeedPath, Feed

if __name__ == "__main__":
    path_sayori_models = "./demo/sayori_models/"
    origin_stop_ids = ["0606-04", "0606-02", "0606-05", "0606-03", "0606-01", "0606-06", "0606-07"]
    departure_date = "2023-11-20"
    departure_time = 6 * 60 * 60
    transfer_limit = 0
    is_reverse_search = False

    feed_path = {
        "trips": f"{path_sayori_models}sayori_trips.parquet",
        "stop_times": f"{path_sayori_models}sayori_stop_times.parquet",
        "stops": f"{path_sayori_models}sayori_stops.parquet",
        "transfers": f"{path_sayori_models}sayori_transfers.parquet",
        "calendar": f"{path_sayori_models}sayori_calendar.parquet"
    }
    feed_path = FeedPath.parse_obj(feed_path)
    feed = Feed.from_feed_path(feed_path)

    req = {
        "origin_stop_ids": origin_stop_ids,
        "specified_date": departure_date,
        "specified_secs": departure_time,
        "transfers_limit": transfer_limit,
        "is_reverse_search": is_reverse_search,
        "available_trip_ids": None
    }
    res = search_isochrones(feed, req)
    print(res)

