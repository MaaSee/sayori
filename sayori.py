#%%
# Python execution sample
from sayori.raptor import search_p2p_geojson
from sayori.models import FeedPath, Feed

# Read feed path
feed_path = load_data_from_path()
# Create feed
feed = Feed.from_feed_path(feed_path)

# Define input data
req = {
    "origin_stop_ids": ['0120_5', '0120_6', '0120_7', '0120_4', '0120_1'],
    "destination_stop_ids": ['0150_2', '0150_4', '0150_1', '0150_3'],
    "specified_date": "2022-09-24",
    "specified_secs": 5 * 60 * 60,
    "transfers_limit": 0,
}

# Execute raptor search
res = search_p2p_geojson(feed, req)
# %%
