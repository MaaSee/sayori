#%%
import time

from .modules.loader import load_data_from_path
from .modules.raptor import search_point_to_point

if __name__ == "__main__":
    # Read feed data
    feed = load_data_from_path()
    #define input data
    # req = {
    #     "origin_stop_id": "0120",
    #     "destination_stop_id": "0150",
    #     "input_date": "2023-05-31",
    #     "input_secs": 5 * 60 * 60,
    #     "transfers_limit": 1,
    # }

    req = {
        "origin_stop_id": "371900",
        "destination_stop_id": "371050",
        "input_date": "2023-05-31",
        "input_secs": 11 * 60 * 60,
        "transfers_limit": 2,
    }
    tic = time.perf_counter()
    res = search_point_to_point(feed, req)
    toc = time.perf_counter()

    print(f"elapsed time of point to point raptor search: {toc - tic} sec." )
    print(res)
#%%
