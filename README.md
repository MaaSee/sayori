# sayori

A raptor based public transportation routing engine.

## Preparation

Set data path of public transportation timetable data in .env file.
A preferable feed data structure is configurated with allocating "raptor_data" directory as a root directory.
Then deploy some directories by data types. A data format should be parquet format. 
An example of directory structure is shown below:

```
.
├── raptor_data
.   ├── calendar
.   │   └── <prefix>_calendar.parquet
.   ├── stop_times
    │   └── <prefix>_stop_times.parquet
    ├── stops
    │   └── <prefix>_stops.parquet
    ├── transfers
    │   └── <prefix>_transfers.parquet
    ├── trips
    │    └── <prefix>_trips.parquet
    └── routes
         └── <prefix>_routes.parquet
```

## Execution

### Point to point route search

Point to point route search is simple routing between two points. 

#### Paremeters

There are seven request fields to execute point to point route search. 

```python
from sayori.raptor import search_point_to_point
from sayori.models import FeedPath, Feed

# Read feed path
feed_path = load_data_from_path()
# Create feed
feed = Feed.from_feed_path(feed_path)

#define input data
req = {
    "origin_stop_ids": ['0120_5', '0120_6', '0120_7', '0120_4', '0120_1'],
    "destination_stop_ids": ['0150_2', '0150_4', '0150_1', '0150_3'],
    "input_date": "2021-09-24",
    "input_secs": 5 * 60 * 60,
    "transfers_limit": 0,
}

# Execute raptor search
res = search_point_to_point(feed, req)
```

| field name | data type | default | descriptions |
|----|----|----|----|
| origin_stop_ids | List[str] | | List of stop_ids of origin point |
| destination_stop_ids | List[str] | | List of stop_id of destination point |
| input_date | str | | A spacific date of route search. The format should be comformed to ISO8601 string |
| input_secs | int | | A specific seconds of route seach |
| transfers_limit | int | | An upper limit of route search round |
| is_reverse_search | bool | False | Set True when execute destination oriented route search |
| available_trip_ids | Optional[List[str]] | None | Set a list of trip_id when execute route search with limited trip_ids |

#### Rerurns

GeoJSON string is returned. This GeoJSON feature represents for routing path with time to reach.

```json
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