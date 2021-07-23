# -------------------------------------------------------------------------------
# Copyright (c) 2017, Battelle Memorial Institute All rights reserved.
# Battelle Memorial Institute (hereinafter Battelle) hereby grants permission to any person or entity
# lawfully obtaining a copy of this software and associated documentation files (hereinafter the
# Software) to redistribute and use the Software in source and binary forms, with or without modification.
# Such person or entity may use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and may permit others to do so, subject to the following conditions:
# Redistributions of source code must retain the above copyright notice, this list of conditions and the
# following disclaimers.
# Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
# the following disclaimer in the documentation and/or other materials provided with the distribution.
# Other than as used herein, neither the name Battelle Memorial Institute or Battelle may be used in any
# form whatsoever without the express written consent of Battelle.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# BATTELLE OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
# OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
# GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
# General disclaimer for use with OSS licenses
#
# This material was prepared as an account of work sponsored by an agency of the United States Government.
# Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any
# of their employees, nor any jurisdiction or organization that has cooperated in the development of these
# materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for
# the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process
# disclosed, or represents that its use would not infringe privately owned rights.
#
# Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer,
# or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United
# States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY operated by BATTELLE for the
# UNITED STATES DEPARTMENT OF ENERGY under Contract DE-AC05-76RL01830
# -------------------------------------------------------------------------------
"""
Created on April 13, 2021

@author: Shiva Poudel
"""""

#from shared.sparql import SPARQLManager
#from shared.glm import GLMManager

import networkx as nx
import pandas as pd
import math
import argparse
import json
import sys
import os
import importlib
import numpy as np
import time
from tabulate import tabulate
import re
from datetime import datetime
# import utils

from gridappsd import GridAPPSD, topics, DifferenceBuilder
from gridappsd.topics import simulation_output_topic, simulation_log_topic, simulation_input_topic

global exit_flag, simulation_id
count = 5

def on_message(headers, message):
    global exit_flag, simulation_id, count
    gapps = GridAPPSD()
    publish_to_topic = simulation_input_topic(simulation_id)
    if type(message) == str:
            message = json.loads(message)

    if 'message' not in message:
        if message['processStatus']=='COMPLETE' or \
           message['processStatus']=='CLOSED':
            print('End of Simulation')
            exit_flag = True

    else:
        meas_data = message["message"]["measurements"]
        timestamp = message["message"]["timestamp"] 
        count += 1
        if count >= 5 :            
            ochre_diff = DifferenceBuilder(simulation_id)
            house_name = 'House_n1'
            forward = {
            'Sol_status_CVXPY__Dimensionless': 1,
            'HVAC Heating': {'Heating Setpoint': 19},  # , 'ER Duty Cycle': 0.1},
            'HVAC Cooling': {'Cooling Duty Cycle': 0.0},
            'Heat Pump Water Heater': {'HP Duty Cycle': 0.0, 'ER Duty Cycle': 0.0},
            'PV': {'P Setpoint': -1.1, 'Q Setpoint': 0.5},
            'Battery': {'P Setpoint': -1.0},
            'Load Fractions': {
                'Air Source Heat Pump': 1,
                'Heat Pump Water Heater': 0,
                'Electric Resistance Water Heater': 0,
                'Scheduled EV': 0,
                'Lighting': 0.2,
                'Exterior Lighting': 0.0,
                'Range': 0.0,
                'Dishwasher': 0.0,
                'Refrigerator': 1.0,
                'Clothes Washer': 0.0,
                'Clothes Dryer': 0.0,
                'MELs': 0.2,
                }
            }
            ochre_diff.add_difference(house_name, "Ochre.command", json.dumps(forward), "")
            msg = ochre_diff.get_message()
            print(msg)
            # gapps.send(publish_to_topic, json.dumps(msg))      


def _main():
    
    global simulation_id
    simulation_id = sys.argv[2]
    feeder_mrid = sys.argv[1]

    # This topic is different for different API
    model_api_topic = "goss.gridappsd.process.request.data.powergridmodel"

    gapps = GridAPPSD()

    config_topic = "goss.gridappsd.process.request.config"
    
    sim_output_topic = simulation_output_topic(simulation_id)
    publish_to_topic = simulation_input_topic(simulation_id)
    # following function allows us to subscribe the simulation output
    # Need a call back function

    ochre_diff = DifferenceBuilder(simulation_id)
    house_name = 'House_n1'
    forward = {
    'Sol_status_CVXPY__Dimensionless': 1,
    'HVAC Heating': {'Heating Setpoint': 19},  # , 'ER Duty Cycle': 0.1},
    'HVAC Cooling': {'Cooling Duty Cycle': 0.0},
    'Heat Pump Water Heater': {'HP Duty Cycle': 0.0, 'ER Duty Cycle': 0.0},
    'PV': {'P Setpoint': -1.1, 'Q Setpoint': 0.5},
    'Battery': {'P Setpoint': -1.0},
    'Load Fractions': {
        'Air Source Heat Pump': 1,
        'Heat Pump Water Heater': 0,
        'Electric Resistance Water Heater': 0,
        'Scheduled EV': 0,
        'Lighting': 0.2,
        'Exterior Lighting': 0.0,
        'Range': 0.0,
        'Dishwasher': 0.0,
        'Refrigerator': 1.0,
        'Clothes Washer': 0.0,
        'Clothes Dryer': 0.0,
        'MELs': 0.2,
        }
    }
    ochre_diff.add_difference(house_name, "Ochre.command", json.dumps(forward), "")
    msg = ochre_diff.get_message()
    print(json.dumps(msg))
    gapps.send(publish_to_topic, json.dumps(msg)) 
    # gapps.subscribe(sim_output_topic, on_message)

    global exit_flag
    exit_flag = False

    while not exit_flag:
        time.sleep(0.1)
    

if __name__ == "__main__":
    _main()