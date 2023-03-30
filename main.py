from RAPTOR.RAPTOR_tweaked import raptor as raptor_tweaked
from miscellaneous_func import *
from RAPTOR.raptor_function_tweaked import *



if __name__ == "__main__":
    # Read network
    FOLDER = './bangalore'
    # print(FOLDER)

    stops_file, trips_file, stop_times_file, transfers_file, stops_dict, stoptimes_dict, footpath_dict, routes_by_stop_dict, idx_by_route_stop_dict, nearest_metro_station_dict, metro_cost_dict, estimated_fare_attributes_file, estimated_fare_rule_file = read_testcase(FOLDER)
    # _ = generate_mapping(stoptimes_dict,stops_file)
    print_network_details(transfers_file, trips_file, stops_file)
    with open('OSM_dist_dict.pkl', 'rb') as file:
        OSM_dist_dict = pickle.load(file)
    with open('stop_OSMnode_mapping.pkl', 'rb') as pickle_file:
        stop_OSMnode_mapping = pickle.load(pickle_file)
    speed = 16 #meter/ssecond

    OSM_dist_dict = {key: pd.to_timedelta(round(value/speed,1), unit="seconds") for key, value in OSM_dist_dict.items()}
    OSM_dist_dict = {key: value.total_seconds() for key, value in OSM_dist_dict.items()}

    stoptimes_dict_modified = {}
    import numpy as np
    for route, stop_list in stops_dict.items():
        travel_time_list = [OSM_dist_dict[(stop_OSMnode_mapping[stop_list[stop_idx]], stop_OSMnode_mapping[stop_list[stop_idx + 1]])] for stop_idx in range(len(stop_list) - 1)]
        # travel_time_list.insert(0, pd.to_timedelta(0))
        travel_time_list.insert(0, 0)
        travel_time_list = np.cumsum(travel_time_list)
        stoptimes_dict_modified[route] = list(zip(stop_list, travel_time_list))

    footpath_dict_m = {stop_p: [(p_dash, time_valie.total_seconds()) for p_dash, time_valie in value] for stop_p, value in footpath_dict.items()}


    D_TIME = pd.to_datetime("2023-01-13 16:00:00")
    print("departure time",D_TIME)
    D_TIME_m = D_TIME.timestamp()
    MAX_TRANSFER = 2
    WALKING_FROM_SOURCE = 1
    CHANGE_TIME_SEC = 0
    PRINT_ITINERARY = 1
    OPTIMIZED = 1
    d_time_groups = stop_times_file.groupby("stop_id")
    time_list = []
    tweak_time, original_time = [], []

    ward_df = pd.read_csv("ward_lat_lon.csv")
    ward_num_list = list(ward_df["ward_no"])
    source_ward_list = []
    destination_ward_list = []
    source_metro_station = []
    destination_metro_station = []
    ivtt = []
    ovtt = []
    waiting_time = []
    transfer_time = []
    metro_fare = []
    access_time = []
    egress_time = []
    num_transfer = []
    for source_ward in range(len(ward_num_list)):
        for destination_ward in range(len(ward_num_list)):
            if ward_num_list[source_ward] != ward_num_list[destination_ward]:
                SOURCE_WARD = ward_num_list[source_ward]
                DESTINATION_WARD = ward_num_list[destination_ward]
                SOURCE_METRO_STOP = nearest_metro_station_dict[SOURCE_WARD][0]
                DESTINATION_METRO_STOP = nearest_metro_station_dict[DESTINATION_WARD][0]
                if SOURCE_METRO_STOP != DESTINATION_METRO_STOP:
                    source_ward_list.append(SOURCE_WARD)
                    source_metro_station.append(SOURCE_METRO_STOP)
                    access_time_cal = nearest_metro_station_dict[SOURCE_WARD][1]/(1.34*60)
                    access_time.append(access_time_cal)
                    destination_ward_list.append(DESTINATION_WARD)
                    destination_metro_station.append(DESTINATION_METRO_STOP)
                    egress_time_cal = nearest_metro_station_dict[DESTINATION_WARD][1]/(1.34*60)
                    egress_time.append(egress_time_cal)
                    output = raptor_tweaked(SOURCE_METRO_STOP, DESTINATION_METRO_STOP, D_TIME_m, MAX_TRANSFER, WALKING_FROM_SOURCE, CHANGE_TIME_SEC,
                                            PRINT_ITINERARY,
                                            routes_by_stop_dict, stops_dict, stoptimes_dict, footpath_dict_m, idx_by_route_stop_dict,
                                            stoptimes_dict_modified, metro_cost_dict)
                    # print("Total time taken",(output[0]["old"][0]-D_TIME_m)/60,"minutes")
                    for transfers, tt_data in output[0]["tt"]:
                        transfer_time.append(tt_data["walk_time"]/60)
                        waiting_time.append(tt_data["wait_time"]/60)
                        ovtt.append((tt_data["ovtt"]/60) + access_time_cal + egress_time_cal)
                        ivtt.append(tt_data["ivtt"]/60)
                        metro_fare.append(tt_data["cost"])
                        num_transfer.append(transfers)

    skim_df = pd.DataFrame(list(zip(source_ward_list, destination_ward_list, source_metro_station, destination_metro_station, ivtt, ovtt, waiting_time, transfer_time, metro_fare, access_time, egress_time, num_transfer)), columns=['source_ward', 'destination_ward', 'source_metro_station', 'destination_metro_station', 'ivtt', 'ovtt', 'waiting_time', 'transfer_time', 'metro_fare', 'access_time', 'egress_time', 'num_transfer'])
    skim_df.to_csv("skim_matrix.csv", index=False)
