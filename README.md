# sayori

A raptor based public transportation routing engine.

# Tutorial
You should specify GTFS file used for sayori backend file. The GTFS should be converted to sayori backend data model.
Run pre_sayori script with the GTFS zipfile as follows. You can choose both on the local directory zipfile and on the http protocol zipfile.

```
# For local directory
poetry run python ./pre_sayori/pre_sayori__gtfs.py ./demo/input_data/ToeiBus-GTFS.zip ./demo/ --stop_id_seperator -

# For http protocol
poetry run python ./pre_sayori/pre_sayori__gtfs.py https://api-public.odpt.org/api/v4/files/Toei/data/ToeiBus-GTFS.zip ./demo/ --stop_id_seperator -
```

Once you got a dataset of sayori backend model, you can run demo script and get isochrone geojson data.

```
poetry run python ./demo/sayori_main.py 
```


## Functions

### Point to point route search

Point to point route search is simple routing between two points. 

#### Paremeters

There are seven request fields to execute point to point route search. 

| field name | data type | default | descriptions |
|----|----|----|----|
| origin_stop_ids | List[str] | | List of stop_ids of origin point |
| destination_stop_ids | List[str] | | List of stop_id of destination point |
| specified_date | str | | A spacific date of route search. The format should be comformed to ISO8601 string |
| specified_secs | int | | A specific seconds of route seach |
| transfers_limit | int | | An upper limit of route search round |
| is_reverse_search | bool | False | Set True when execute destination oriented route search |
| available_trip_ids | Optional[List[str]] | None | Set a list of trip_id when execute route search with limited trip_ids |

#### Returns

GeoJSON string is returned. This GeoJSON feature represents for routing path with time to reach.

| field name | description | 
|----|----|
| type | "FeatureCollection". See RFC7946. |
| features[].type | "Feature". See RFC7946. |
| features[].geometry.type | "LineString" | 
| features[].geometry.coordinates | A sequence of longitude and latitude pairs are assgined. |
| features[].properties.time_to_reach | An estimated duration of desinated point to point. |
| features[].properties.routing_path | A sequence of stop_ids, which represents the way of routing path. |
| features[].properties.preceding | The trip_ids used by route search. |
| features[].properties.stop_id | A final reached stop_id as a result of routing. |

#### Example

```python
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
    "specified_date": "2021-09-24",
    "specified_secs": 5 * 60 * 60,
    "transfers_limit": 0,
}

# Execute raptor search
res = search_p2p_geojson(feed, req)
```


```json
// A GeoJSON returns sample
{
    "type": "FeatureCollection", 
    "features": [
        {
            "type": "Feature", 
            "geometry": {
                "type": "LineString", 
                "coordinates": [
                    [130.522563, 33.496030000000005], 
                    [130.542415, 33.471615], 
                    [130.51788499999998, 33.502145], 
                    [130.532605, 33.484025], 
                    [130.55295999999998, 33.462810000000005]
                ]
            }, 
            "properties": {
                "time_to_reach": 1610, 
                "routing_path": ["0120_5", "0125_1", "0130_1", "0140_1", "0150_2"], 
                "preceding": ["A083", "2071", "G363", "2063", "1051"], 
                "stop_id": "0150_2"
            }
        }
    ]
}
```


