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
    └── trips
        └── <prefix>_trips.parquet
```

## Execution

### Point to point route search

There are seven request fields to execute point to point route search. 

| field name | data type | default | descriptions |
|----|----|----|----|
| origin_stop_id | str | | parent_station id of origin point |
| destination_stop_id | str | | parent_station id of destination point |
| input_date | str | | A spacific date of route search. The format should be comformed to ISO8601 string |
| input_secs | int | | A specific seconds of route seach |
| transfers_limit | int | | An upper limit of route search round |
| is_reverse_search | bool | False | Set True when execute destination oriented route search |
| available_trip_ids | Optional[List[str]] | None | Set a list of trip_id when execute route search with limited trip_ids |
