"""
Module defines a function to save (using pickling) the GTFS information in the form of a dictionary.
This is done for easy/faster data lookup.
"""

import pickle
import pandas as pd
from tqdm import tqdm
from sklearn.neighbors import BallTree
from haversine import haversine, Unit

def build_save_route_by_stop(stop_times_file, FOLDER: str) -> dict:
    """
    This function saves a dictionary to provide easy access to all the routes passing through a stop_id.

    Args:
        stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
        FOLDER (str): path to network folder.

    Returns:
        route_by_stop_dict_new (dict): keys: stop_id, values: list of routes passing through the stop_id. Format-> dict[stop_id] = [route_id]
    """
    print("building routes_by_stop")
    stops_by_route = stop_times_file.drop_duplicates(subset=['route_id', 'stop_sequence'])[['stop_id', 'route_id']].groupby('stop_id')
    route_by_stop_dict_new = {id: list(routes.route_id) for id, routes in stops_by_route}

    with open(f'./dict_builder/{FOLDER}/routes_by_stop.pkl', 'wb') as pickle_file:
        pickle.dump(route_by_stop_dict_new, pickle_file)
    print("routes_by_stop done")
    return route_by_stop_dict_new


def build_save_stops_dict(stop_times_file, trips_file, FOLDER: str)-> dict:
    """
    This function saves a dictionary to provide easy access to all the stops in the route.

    Args:
        stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
        trips_file (pandas.dataframe): trips.txt file in GTFS.
        FOLDER (str): path to network folder.

    Returns:
        stops_dict (dict): keys: route_id, values: list of stop id in the route_id. Format-> dict[route_id] = [stop_id]
    """
    print("building stops dict")
    trips_group = stop_times_file.groupby("trip_id")  # This drops all trips for which timestamps are not sorted
    trips_with_correct_timestamps = [id for id, trip in tqdm(trips_group) if list(trip.arrival_time) == list(trip.arrival_time.sort_values())]
    if len(trips_with_correct_timestamps) != trips_file.shape[0]:
        print(f"Incorrect time sequence in stoptimes builder file")

    stop_times = stop_times_file[stop_times_file["trip_id"].isin(trips_with_correct_timestamps)]
    route_groups = stop_times.drop_duplicates(subset=['route_id', 'stop_sequence'])[['stop_id', 'route_id', 'stop_sequence']].groupby('route_id')
    stops_dict = {id: routes.sort_values(by='stop_sequence')['stop_id'].to_list() for id, routes in route_groups}

    with open(f'./dict_builder/{FOLDER}/stops_dict_pkl.pkl', 'wb') as pickle_file:
        pickle.dump(stops_dict, pickle_file)
    print("stops_dict done")
    return stops_dict


def build_save_stopstimes_dict(stop_times_file, trips_file, FOLDER: str) -> dict:
    """
    This function saves a dictionary to provide easy access to all the trips passing along a route id. Trips are sorted
    in the increasing order of departure time. A trip is list of tuple of form (stop id, arrival time)

    Args:
        stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
        trips_file (pandas.dataframe): dataframe with transfers (footpath) details.
        FOLDER (str): path to network folder.

    Returns:
        stoptimes_dict (dict): keys: route ID, values: list of trips in the increasing order of start time. Format-> dict[route_ID] = [trip_1, trip_2] where trip_1 = [(stop id, arrival time), (stop id, arrival time)]
    """
    print("building stoptimes dict")

    stop_times_file.arrival_time = pd.to_datetime(stop_times_file.arrival_time)
    route_group = stop_times_file.groupby("route_id")
    stoptimes_dict = {r_id: [] for r_id, _ in route_group}
    for r_id, route in tqdm(route_group):
        trip_group = route.groupby("trip_id")  # Collect trip start points
        temp = route[route.stop_sequence == 1][["trip_id", "arrival_time"]].sort_values(by=["arrival_time"])
        for trip_id in temp["trip_id"]:  # Add them inorder
            trip = trip_group.get_group(trip_id).sort_values(by=["stop_sequence"])
            stoptimes_dict[r_id].append(list(zip(trip.stop_id, trip.arrival_time)))

    with open(f'./dict_builder/{FOLDER}/stoptimes_dict_pkl.pkl', 'wb') as pickle_file:
        pickle.dump(stoptimes_dict, pickle_file)
    print("stoptimes dict done")
    return stoptimes_dict


def build_save_footpath_dict(transfers_file, FOLDER: str)-> dict:
    """
    This function saves a dictionary to provide easy access to all the footpaths through a stop id.

    Args:
        transfers_file (pandas.dataframe): dataframe with transfers (footpath) details.
        FOLDER (str): path to network folder.

    Returns:
        footpath_dict (dict): keys: from stop_id, values: list of tuples of form (to stop id, footpath duration). Format-> dict[stop_id]=[(stop_id, footpath_duration)]
    """
    print("building footpath dict..")
    footpath_dict = {}
    g = transfers_file.groupby("from_stop_id")
    for from_stop, details in tqdm(g):
        footpath_dict[from_stop] = []
        for _, row in details.iterrows():
            footpath_dict[from_stop].append((row.to_stop_id, pd.to_timedelta(float(row.min_transfer_time), unit='seconds')))

    with open(f'./dict_builder/{FOLDER}/transfers_dict_full.pkl', 'wb') as pickle_file:
        pickle.dump(footpath_dict, pickle_file)
    print("transfers_dict done")
    return footpath_dict

def stop_idx_in_route(stop_times_file, FOLDER: str)-> dict:
    """
    This function saves a dictionary to provide easy access to index of a stop in a route.

    Args:
        stop_times_file (pandas.dataframe): stop_times.txt file in GTFS.
        FOLDER (str): path to network folder.

    Returns:
        idx_by_route_stop_dict (dict): Keys: (route id, stop id), value: stop index. Format {(route id, stop id): stop index in route}.
    """
    pandas_group = stop_times_file.groupby(["route_id","stop_id"])
    idx_by_route_stop = {route_stop_pair:details.stop_sequence.iloc[0] for route_stop_pair, details in pandas_group}

    with open(f'./dict_builder/{FOLDER}/idx_by_route_stop.pkl', 'wb') as pickle_file:
        pickle.dump(idx_by_route_stop, pickle_file)
    print("idx_by_route_stop done")
    return idx_by_route_stop

def build_nearest_metro_station_dict(stops_file, ward_df, FOLDER):
    print("building Nearest metro dict")
    metro_station_ids = list(stops_file["stop_id"])
    metro_station_points = [[stops_file.iloc[row].stop_lat, stops_file.iloc[row].stop_lon] for row in range(stops_file.shape[0])]
    tree_ball = BallTree(metro_station_points, metric="haversine")

    ward_no, ward_lat_lon = [], []
    for row in range(ward_df.shape[0]):
        ward_no.append(ward_df["ward_no"].iloc[row])
        ward_lat_lon.append([ward_df["ward_lat"].iloc[row], ward_df["ward_lon"].iloc[row]])

    nearest_metro_station_dict = {}
    for i in range(len(ward_lat_lon)):
        print(i)
        ward_point = [ward_lat_lon[i]]
        distance, index = tree_ball.query(ward_point, k=1)
        nearest_metro_station_id = metro_station_ids[index[0][0]]
        nearest_metro_point = metro_station_points[index[0][0]]
        distance_to_nearest_metro_station = haversine(ward_point[0], nearest_metro_point, unit=Unit.METERS)
        nearest_metro_station_dict[ward_no[i]] = (nearest_metro_station_id, distance_to_nearest_metro_station)

    with open(f'./dict_builder/{FOLDER}/nearest_metro_station_dict.pkl', 'wb') as pickle_file:
        pickle.dump(nearest_metro_station_dict, pickle_file)
    print("end of building nearest metro dict")
    return nearest_metro_station_dict

def build_metro_cost_dict(estimated_fare_attributes_file, estimated_fare_rule_file, FOLDER):
    metro_cost_dict = {}
    for row in range(estimated_fare_rule_file.shape[0]):
        tup = (estimated_fare_rule_file.iloc[row].origin_id, estimated_fare_rule_file.iloc[row].destination_id)
        fare_id = estimated_fare_rule_file.iloc[row].fare_id
        cost = float(estimated_fare_attributes_file[estimated_fare_attributes_file.fare_id == fare_id].iloc[0].price)
        metro_cost_dict[tup] = cost

    with open(f'./dict_builder/{FOLDER}/metro_cost_dict.pkl', 'wb') as pickle_file:
        pickle.dump(metro_cost_dict, pickle_file)

    return metro_cost_dict
