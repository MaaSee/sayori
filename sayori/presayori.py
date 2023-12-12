#%%
import os
import io
import argparse
from typing import Optional, Sequence

import httpx
import pandas as pd
import polars as pl
import zipfile

def read_csv(fp, has_header:bool = True, new_columns: Optional[Sequence[str]] = None, encoding: str = "utf8"):
    return (
        pl.read_csv(
            fp, 
            encoding = encoding,
            infer_schema_length = 0,
            has_header = has_header,
            new_columns = new_columns
        )
        .with_columns(
            pl.all().cast(pl.Utf8, strict=False)
        )
    )

def get_agency(timetables: pl.DataFrame) -> pl.DataFrame:
    """agencyの作成"""
    sayori_agency = (   
        timetables
        .select(
            pl.col("agency_id"),
            pl.col("agency_name")
        )
        .unique()
    )
    return sayori_agency

def get_routes(timetables: pl.DataFrame) -> pl.DataFrame:
    """routesの作成"""
    sayori_routes = (
        timetables
        .select(
            "route_id",
            "route_long_name",
            "route_short_name",
            "route_desc",
            "route_type",
            "agency_id"
        )
        .unique(subset=["route_id", "agency_id"])
    )
    return sayori_routes

def get_trips(timetables: pl.DataFrame) -> pl.DataFrame:
    """tripsの作成"""
    sayori_trips = (
        timetables
        .select(
            pl.col("trip_id"),
            pl.col("route_id"),
            pl.col("service_id"),
            pl.col("trip_headsign"),
            pl.col("trip_short_name"),
            pl.col("block_id"),
            pl.col("agency_id"),
        )
        .unique(subset=["trip_id", "agency_id"])
    )
    # TODO: Switching mechanism should be installed
    # return sayori_trips
    return sayori_trips.select("trip_id", "route_id", "service_id", "trip_headsign", "trip_short_name", "block_id")


def get_stop_times(timetables: pl.DataFrame) -> pl.DataFrame:
    """stoptimesの作成"""
    sayori_stop_times = (
        timetables
        .select(
            pl.col("trip_id"),
            pl.col("stop_sequence").cast(pl.Int64),
            pl.col("stop_id"),
            pl.col("arrival_time").str.split(":"),
            pl.col("departure_time").str.split(":"),
            pl.col("pickup_type").cast(pl.Int32),
            pl.col("drop_off_type").cast(pl.Int32),
            pl.col("agency_id")
        )
        .with_columns(
            pl.col("arrival_time").list.get(0).cast(pl.Int32) * 60 * 60 + pl.col("arrival_time").list.get(1).cast(pl.Int32) * 60,
            pl.col("departure_time").list.get(0).cast(pl.Int32) * 60 * 60 + pl.col("departure_time").list.get(1).cast(pl.Int32) * 60,
            pl.when(pl.col("pickup_type").is_null()).then(pl.lit(0)).otherwise(pl.col("pickup_type")).alias("pickup_type"),
            pl.when(pl.col("drop_off_type").is_null()).then(pl.lit(0)).otherwise(pl.col("drop_off_type")).alias("drop_off_type"),
        )

    )

    # TODO: Switching mechanism should be installed
    # return sayori_stop_times
    return sayori_stop_times.select("trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time", "pickup_type", "drop_off_type")


def get_stops(timetables: pl.DataFrame, stop_id_seperator: str = " ") -> pl.DataFrame:
    """stopsの作成"""
    sayori_stops = (
        timetables
        .group_by(["stop_id", "stop_name",  "location_type", "agency_id"])
        .agg(
            pl.col("parent_station"),
            pl.col("platform_code").first(),
            pl.col("stop_lat"),
            pl.col("stop_lon"),
        )
        .with_columns(
            pl.when(pl.col("parent_station").is_null()).then(pl.col("stop_id").str.split(stop_id_seperator).list.get(0))
            .otherwise(pl.col("parent_station"))
            .alias("parent_station"),
            pl.col("stop_lat").list.first().cast(pl.Float64),
            pl.col("stop_lon").list.first().cast(pl.Float64),
        )
    )
    # TODO: Switching mechanism should be installed
    # return sayori_stops
    return sayori_stops.select("stop_id", "stop_name", "parent_station", "platform_code", "stop_lat", "stop_lon")


def get_calendar(
    timetables: pl.DataFrame, 
    int_calendar: pl.DataFrame, 
    int_calendar_dates: Optional[pl.DataFrame] = None 
) -> pl.DataFrame:

    dayofweek = {
        'monday': 1, 
        'tuesday': 2, 
        'wednesday': 3, 
        'thursday': 4, 
        'friday': 5, 
        'saturday': 6, 
        'sunday': 7
    }

    sayori_calendar = []
    for dow_name, dow_num  in dayofweek.items():
        sayori_calendar.append(
            int_calendar
            .filter(pl.col(dow_name) == "1")
            .with_columns(pl.lit(dow_num).alias("dayofweek"))
            .select(
                "service_id",
                "calendar_date",
                "dayofweek"
            )
        )

    sayori_calendar = pl.concat(sayori_calendar)
    sayori_calendar = (
        sayori_calendar
        .explode("calendar_date")
        .with_columns(pl.col("calendar_date").dt.weekday().alias("weekday"))
        .filter(pl.col("dayofweek") == pl.col("weekday"))
        .select(
            "service_id",
            "calendar_date"
        )
    )

    if isinstance(int_calendar_dates, pl.DataFrame):
        # Add exception_type 1 dates
        sayori_calendar = pl.concat([
            sayori_calendar,
            (
                int_calendar_dates
                .filter(pl.col("exception_type") == "1")
                .select(
                    "service_id",
                    "calendar_date",
                )
            )
        ])

        # Exclude exception_type 2 dates
        sayori_calendar = (
            sayori_calendar
            .join(
                int_calendar_dates
                .filter(pl.col("exception_type") == "2")
                .select(
                    "service_id",
                    "calendar_date",
                ),
                on = ["service_id", "calendar_date"],
                how = "anti" 
            )
        )

    sayori_calendar = (
        timetables
        .select("service_id", "agency_id")
        .unique()
        .join(
            sayori_calendar,
            on = "service_id"
        )
    )

    sayori_calendar = (
        sayori_calendar
        .group_by(["calendar_date", "agency_id"])
        .agg(pl.col("service_id").alias("service_ids"))
    )

    return sayori_calendar.select("calendar_date", "service_ids")

def get_transfers(sayori_stops, min_transfer_time: int = 1):
    stops = sayori_stops.select("stop_id", "parent_station")
    
    sayori_transfers = ( 
        stops
        .join(stops, on="stop_id", how="cross")
        .filter(pl.col("parent_station") == pl.col("parent_station_right"))
        .select([
            pl.col("stop_id").alias("from_stop_id"),
            pl.col("stop_id_right").alias("to_stop_id"),
            pl.lit(0).cast(pl.Int32).alias("transfer_type"),
            pl.lit(min_transfer_time).cast(pl.Int32).alias("min_transfer_time"),
        ])
    )
    return sayori_transfers

def read_gtfs_feed(fp: str):
    def read_gtfs_zipfile(file):
        with zipfile.ZipFile(file) as z:
            filenames = [file.filename for file in z.filelist]
            if "calendar_dates.txt" in filenames:
                required_feeds = ["agency.txt", "routes.txt", "trips.txt", "stop_times.txt", "stops.txt", "calendar.txt", "calendar_dates.txt"]
            else:
                required_feeds = ["agency.txt", "routes.txt", "trips.txt", "stop_times.txt", "stops.txt", "calendar.txt"]
            
            if set(required_feeds).issubset(set(filenames)):
                gtfs_feeds = []
                for filename in required_feeds:
                    file = z.open(filename, "r")
                    gtfs_feed = pd.read_csv(file, dtype = str)

                    cols = gtfs_feed.columns
                    if filename == "routes.txt":
                        if "route_long_name" not in cols:
                            gtfs_feed["route_long_name"] = None
                        elif "route_short_name" not in cols:
                            gtfs_feed["route_short_name"] = None
                        
                        if "route_desc" not in cols:
                            gtfs_feed["route_desc"] = None
                    
                    elif filename == "stops.txt":
                        if "parent_station" not in cols:
                            gtfs_feed["parent_station"] = None
                        
                        if "platform_code" not in cols:
                            gtfs_feed["platform_code"] = None

                    gtfs_feeds.append(pl.from_pandas(gtfs_feed)) 
            else:
                missing_feeds = set(required_feeds) - set(filenames)
                raise FileNotFoundError(f"""Required GTFS feed is missing: {",".join(missing_feeds)}""")
        
        if "calendar_dates.txt" in filenames:
            return gtfs_feeds
        else:
            return *gtfs_feeds, None


    if fp.startswith("http"):
        req = httpx.get(fp, follow_redirects=True)
        if req.status_code == 200:
            file = io.BytesIO(req.content)
        else:
            raise FileNotFoundError(f"Status code {req.status_code} is returned")
        
        if zipfile.is_zipfile(file):
            gtfs_feeds = read_gtfs_zipfile(file)
        else:
            raise AttributeError(f"""The specified data is not a zipfile: {fp}""")
            
    else:
        if zipfile.is_zipfile(fp):
            gtfs_feeds = read_gtfs_zipfile(fp)
        else:
            raise AttributeError(f"""The specified data is not a zipfile: {fp}""")
    
    return gtfs_feeds


def gtfs(filepath, output_path, stop_id_seperator):
    gtfs_agency, gtfs_routes, gtfs_trips, gtfs_stop_times, gtfs_stops, gtfs_calendar, gtfs_calendar_dates = read_gtfs_feed(filepath)

    intermidiate_timetables = (
        gtfs_trips
        .join(
            gtfs_routes,
            on = "route_id"
        )
        .join(
            gtfs_stop_times,
            on = "trip_id"
        )
        .join(
            gtfs_agency,
            on = "agency_id"
        )
        .join(
            gtfs_stops,
            on = "stop_id"
        )
    )

    intermidiate_calendar = (
        gtfs_calendar
        .select(
            "service_id",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            pl.date_ranges(
                pl.col("start_date").str.to_date(format="%Y%m%d"),
                pl.col("end_date").str.to_date(format="%Y%m%d"),
                "1d"
            ).alias("calendar_date")
        )    
    )

    if isinstance(gtfs_calendar_dates, pl.DataFrame):
        intermidiate_calendar_dates = (
            gtfs_calendar_dates
            .select(
                "service_id",
                pl.col("date").str.to_date(format="%Y%m%d").alias("calendar_date"),
                pl.col("exception_type")
            )    
        )

    sayori_models = {}
    sayori_models["agency"] = get_agency(intermidiate_timetables)
    sayori_models["routes"] = get_routes(intermidiate_timetables)
    sayori_models["trips"] = get_trips(intermidiate_timetables)
    sayori_models["stop_times"] = get_stop_times(intermidiate_timetables)
    sayori_models["stops"] = get_stops(intermidiate_timetables, stop_id_seperator)
    sayori_models["transfers"] = get_transfers(sayori_models["stops"], 1)

    if isinstance(gtfs_calendar_dates, pl.DataFrame):
        sayori_models["calendar"] = get_calendar(intermidiate_timetables, intermidiate_calendar, intermidiate_calendar_dates)
    else:
        sayori_models["calendar"] = get_calendar(intermidiate_timetables, intermidiate_calendar)

    if not os.path.exists(f"{output_path}sayori_models/"):
        os.mkdir(f"{output_path}sayori_models/")
    for sayori_model_name, sayori_model in sayori_models.items():
        sayori_model.write_parquet(f"{output_path}sayori_models/sayori_{sayori_model_name}.parquet")


#%%
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""pre_sayori__gtfs is a file conversion script for sayori""")
    parser.add_argument("filepath", help="""Set a GTFS zipfile path  e.g.) ./demo/input_data/ToeiBus-GTFS.zip """)
    parser.add_argument("output_path", help="""Set a converted data output path  e.g.) ./demo/ """)
    parser.add_argument("--stop_id_seperator", help="""Set stop_id seperator string  e.g.) - """)
    args = parser.parse_args()

    filepath = args.filepath
    output_path = args.output_path
    stop_id_seperator = args.stop_id_seperator

    gtfs(filepath, output_path, stop_id_seperator)

# %%