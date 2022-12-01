from gridappsd.difference_builder import DifferenceBuilder
from gridappsd.simulation import Simulation
from gridappsd import topics as t
import pandas as pd
import numpy as np
import math
import json

class SimulationClass(object):
    # https://gridappsd-training.readthedocs.io/en/develop/api_usage/3.6-Controlling-Simulation-API.html

    def __init__(self, gapps_obj, config_path):
        # Instantiate class with specific initial state

        # Attributes required by Simulation API
        self._finished = False
        self._gapps = gapps_obj
        self._sim_config = json.load(open(config_path))
        self._feeder_mrid = self._sim_config["power_system_config"]["Line_name"]
        self._simulation = Simulation(self._gapps,self._sim_config)
        self._simulation.add_oncomplete_callback(self.onComplete)
        self._simulation.add_onmeasurement_callback(self.onMeasurment)
        self._simulation.add_onstart_callback(self.onStart)
        self._simulation.add_ontimestep_callback(self.onTimestep)
        self._simulation.start_simulation()

        self.getSwitchMeasurments()
        self.getLoadMeasurments()

        # following function allows us to subscribe the simulation output
        # Need a call back function
        self._gapps.subscribe(t.simulation_output_topic(self._simulation.simulation_id), self.onMessage)

        # Attributes to publish difference measurements
        # self.diff = DifferenceBuilder(simulation_id)

    def onStart(self, sim):
        # Use extra methods to subscribe to other topics, such as simulation logs
        print(f"The simulation has started with id : {self._simulation.simulation_id}")

    def onMeasurment(self, sim, timestamp, measurements):
        print(f"A measurement was taken with timestamp : {timestamp}")
        # Print the switch status just once
        switch_data = self._switch_df[self._switch_df['eqid'] == '_6C1FDA90-1F4E-4716-BC90-1CCB59A6D5A9']
        print(switch_data)
        for k in switch_data.index:
            measid = switch_data['measid'][k]
            status = measurements[measid]['value']
            print(switch_data, status)
    
    def onTimestep(self, sim, timestep):
        print(f"A timestep was taken of : {timestep}")

    def onComplete(self, sim):
        print("The simulation has finished")
        self._finished = True

    def onMessage(self, headers, message):
        if type(message) == str:
            message = json.loads(message)

        if 'message' not in message:
            if message['processStatus'] == 'COMPLETE' or message['processStatus'] == 'CLOSED':
                print('End of Simulation')
                self._finished = True
        else:
            meas_data = message["message"]["measurements"]
            
            # # Print the switch status just once
            # switch_data = self._switch_df[self._switch_df['eqid'] == '_6C1FDA90-1F4E-4716-BC90-1CCB59A6D5A9']
            # print(switch_data)
            # for k in switch_data.index:
            #     measid = switch_data['measid'][k]
            #     status = meas_data[measid]['value']
            #     print(switch_data, status)

            # for k in range(self._load_df.shape[0]):
            #     measid = self._load_df['measid'][k]
            #     pq = meas_data[measid]
            #     phi = (pq['angle']) * math.pi / 180
            #     kW = 0.001 * pq['magnitude'] * np.cos(phi)
            #     kVAR = 0.001 * pq['magnitude'] * np.sin(phi)
            #     print(kW, kVAR)

    def getSwitches(self):
        message = {
            "modelId": self._feeder_mrid,
            "requestType": "QUERY_OBJECT_DICT",
            "resultFormat": "JSON",
            "objectType": "LoadBreakSwitch"
        }
        sw_dict = self._gapps.get_response(t.REQUEST_POWERGRID_DATA, message, timeout=10)
        print(sw_dict)

    def getSwitchMeasurments(self):
        message = {
            "modelId": self._feeder_mrid,
            "requestType": "QUERY_OBJECT_MEASUREMENTS",
            "resultFormat": "JSON",
            "objectType": "LoadBreakSwitch"
        }
        switch_data = self._gapps.get_response(t.REQUEST_POWERGRID_DATA, message, timeout=10)
        switch_data = switch_data['data']

        # Filter the response based on type
        switch_data = [e for e in switch_data if e['type'] == 'Pos']
        self._switch_df = pd.DataFrame(switch_data)
        print(self._switch_df)

    def getLoadMeasurments(self):
        message = {
            "modelId": self._feeder_mrid,
            "requestType": "QUERY_OBJECT_MEASUREMENTS",
            "resultFormat": "JSON",
            "objectType": "ACLineSegment"
        }
        load_data = self._gapps.get_response(t.REQUEST_POWERGRID_DATA, message, timeout=10)
        load_data = load_data['data']

        # Filter the response based on type
        load_data = [l for l in load_data if l['type'] == 'VA']
        self._load_df = pd.DataFrame(load_data)
        print(self._load_df)