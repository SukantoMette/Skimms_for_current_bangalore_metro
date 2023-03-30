"""
Contains definition of classes for representing journey.
"""
import datetime
import pandas as pd

class Journey:
    """
    Represents an entire journey.
    Note:
    Works without the departure time being given as input.
    If the departure time is not given, the initial waiting time
    is ignored, if the first leg of the journey is not walking.
    Attributes
    ----------
    transfers (int): the number of transfers in the journey.
    journey_start_time (datetime.datetime): the starting time.
    journey_seq (list[Legs]): list of steps in the journey.
    Methods
    -------
    get_walk_time(self) -> float:
        returns total walking time in seconds.
    get_wait_time(self) -> float:
        returns total wait time in seconds.
    get_ovtt(self) -> float:
        returns outside vehicle travel time in seconds,
        which is the sum of the walk_time and wait_time.
    get_ivtt(self) -> float:
        returns inside vehicle travel time in seconds.
    """

    def __init__(self, transfers: int, journey: list, D_TIME=None):
        """
        Parameters
        ----------
        transfers (int): the number of transfers.
        journey (list): sequence of `pointer_labels' that make up the
                        journey.
        D_TIME (datetime.datetime): starting time of the journey(optional)
        """
        self.transfers = transfers
        if D_TIME is not None:
            self.journey_start_time = D_TIME.to_pydatetime()

        else:
            self.journey_start_time = self._get_pseudo_start_time(journey)
        self.journey_seq = []

        start_time = pd.to_datetime(self.journey_start_time, unit="s") # datetime

        for leg in journey:
            if leg[0] == 'walking':
                mode = 'walk'
                duration = leg[3] # in seconds
                end_time = pd.to_datetime(leg[4], unit="s") # datatime
                start_id = leg[1]  # stop-id
                stop_id = leg[2]  # stop_id

                thisLeg = Leg(
                    mode, start_time, end_time,
                    duration, start_id, stop_id
                )
                self.journey_seq.append(thisLeg)
                start_time = end_time

            else:
                mode = 'other'
                start_time = pd.to_datetime(leg[0], unit="s") # datetime
                end_time = pd.to_datetime(leg[3], unit="s")  # datetime
                start_id = leg[1]
                stop_id = leg[2]
                trip_id = leg[4]
                duration = end_time-start_time # in seconds

                thisLeg = Leg(
                    mode, start_time, end_time,
                    duration, start_id, stop_id, trip_id
                )
                self.journey_seq.append(thisLeg)
                start_time = end_time

    def _get_pseudo_start_time(self, journey_list):
        first_leg = journey_list[0]

        if first_leg[0] == "walking":
            end_time = pd.to_datetime(first_leg[4], unit="s")
            duration = first_leg[3]
            start_time = end_time - datetime.timedelta(seconds=duration)

        else:
            start_time = pd.to_datetime(first_leg[0], unit="s")

        return start_time

    def get_walk_time(self) -> float:
        """
        returns total walking time in seconds.
        """
        tt = 0
        for leg in self.journey_seq:
            if leg.mode == 'walk':
                tt += leg.duration

        return round(tt, 2)

    def get_wait_time(self) -> float:
        """
        returns total wait time in seconds.
        """
        wt = 0
        prev_end_time = self.journey_start_time
        for leg in self.journey_seq:
            wt += (leg.start_time - prev_end_time).total_seconds()
            prev_end_time = leg.end_time

        return round(wt, 2)

    def get_ovtt(self) -> float:
        """
        returns outside vehicle travel time in seconds,
        which is the sum of the walk_time and wait_time.
        """
        return round(self.get_walk_time() + self.get_wait_time(), 2)

    def get_ivtt(self) -> float:
        """
        returns inside vehicle travel time in seconds.
        """
        tt = 0
        for leg in self.journey_seq:
            # print()
            # print(leg)
            # print(leg.start_time)
            # print(leg.mode)
            # print(leg.duration)
            # print(leg.trip_id)
            # print(leg.end_time)
            # print(leg.stop_id)
            # print(leg.start_id)
            if leg.mode != 'walk':
                tt += leg.duration.total_seconds()

        return round(tt, 2)
    def get_metro_cost(self, metro_cost_dict) -> float:
        cost = 0
        for leg in self.journey_seq:
            if leg.mode != "walk":
                cost += metro_cost_dict[(leg.start_id, leg.stop_id)]
        return cost

    def __str__(self):
        leg_list = [leg.__str__() for leg in self.journey_seq]
        return '\n'.join(leg_list)



class Leg:
    """
    Class for representing a step of the journey.
    Attributes
    ----------
    mode (str): is either `walk' or `other'.
    start_time (datetime.datetime): start time of the step.
    end_time (datetime.datetime): end time of the step.
    duration (float): duration of the trip in seconds.
    start_id (int): stop_id of the starting point.
    stop_id (int): stop_id of the ending point.
    trip_id (str): trip_id of the trip. Is None if mode is `walk'.
    """

    def __init__(self, mode: str, start_time, end_time,
                 duration: float, start_id: int, stop_id:int,
                 trip_id=None):
        """
        Parameters
        ----------
        mode (str): `walk' or `other'.
        start_time (datetime.datetime): start time of the step.
        end_time (datetime.datetime): end time of the step.
        duration (float): duration of the trip in seconds.
        start_id (int): `stop_id' of the initial point.
        stop_id (int): `stop_id' of the ending point.
        trip_id (str/None): `trip_id' of the trip. If mode is walking,
                             this is None.
        """
        self.mode = mode
        self.start_time = start_time
        self.end_time = end_time
        self.duration = duration
        self.start_id = start_id
        self.stop_id = stop_id
        self.trip_id = trip_id


    def __str__(self):
        return_val = ''
        if self.mode == 'walk':
            return_val = ("from {start_id} walk till {stop_id} for "
                          "{duration} seconds").format(
                              start_id=self.start_id,
                              stop_id=self.stop_id,
                              duration=self.duration
                          )

        else:
            return_val = ("from {start_id} board at {start_time} and "
                          "get down on {stop_id} at {end_time} "
                          "along {trip_id}").format(
                              start_id=self.start_id,
                              start_time=self.start_time.time(),
                              stop_id=self.stop_id,
                              end_time=self.end_time.time(),
                              trip_id=self.trip_id
                          )
        return return_val