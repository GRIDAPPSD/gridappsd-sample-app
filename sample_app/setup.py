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
Created on Jan 19, 2018

@author: Craig Allwardt, Shiva Poudel
"""

__version__ = "0.0.8"

import argparse
import json
import logging
import sys
import time
from top_identify import Topology
from get_Load import PowerData
from restoration_WSU import Restoration
from mrid_map import SW_MRID
from Isolation import OpenSw
from model_query import MODEL_EQ


from gridappsd import GridAPPSD, DifferenceBuilder, utils, GOSS
from gridappsd.topics import simulation_input_topic, simulation_output_topic, simulation_log_topic, simulation_output_topic

DEFAULT_MESSAGE_PERIOD = 5
message_period = 3
logging.getLogger('stomp.py').setLevel(logging.ERROR)

_log = logging.getLogger(__name__)


class SwitchingActions(object):
    """ A simple class that handles publishing forward and reverse differences

    The object should be used as a callback from a GridAPPSD object so that the
    on_message function will get called each time a message from the simulator.  During
    the execution of on_meessage the `SwitchingActions` object will publish a
    message to the simulation_input_topic with the forward and reverse difference specified.
    """

    def __init__(self, simulation_id, gridappsd_obj, switches, msr_mrids_loadsw, msr_mrids_demand, demand, line):
        """ Create a ``SwitchingActions`` object

        This object should be used as a subscription callback from a ``GridAPPSD``
        object.  This class will toggle the switches passed to the constructor
        off and on based on isolation and restoration that are received on the ``fncs_output_topic``.

        Note
        ----
        This class does not subscribe only publishes.

        Parameters
        ----------
        simulation_id: str
            The simulation_id to use for publishing to a topic.
        gridappsd_obj: GridAPPSD
            An instatiated object that is connected to the gridappsd message bus
            usually this should be the same object which subscribes, but that
            isn't required.
            A list of switches mrids to turn on/off and DGs to create islands
        """
        self._gapps = gridappsd_obj
        self._flag = 0
        self._store = []
        self._message_count = 0
        self._last_toggle_on = False
        self._open_diff = DifferenceBuilder(simulation_id)
        self._close_diff = DifferenceBuilder(simulation_id)
        self._publish_to_topic = simulation_input_topic(simulation_id)
        self.msr_mrids_loadsw = msr_mrids_loadsw
        self.msr_mrids_demand = msr_mrids_demand
        self.LineData = line
        self.DemandData  = demand
        self.switches  = switches
        self.TOP = []
        self.flag_res = 0
        _log.info("Building cappacitor list")

        
    def on_message(self, headers, message):
        """ Handle incoming messages on the simulation_output_topic for the simulation_id

        Parameters
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object
            A data structure following the protocol defined in the message structure
            of ``GridAPPSD``.  Most message payloads will be serialized dictionaries, but that is
            not a requirement.
        """

        self._message_count += 1
        flag_fault = 0
        flag_event = 0

        # Checking the topology everytime communicating with the platform
        if self.flag_res == 0:
            top = Topology(self.msr_mrids_loadsw, self.switches, message, self.TOP, self.LineData)
            TOP, flag_event = top.curr_top()
            self.TOP = TOP

        #Locate fault
        if flag_event == 1:
            flag_fault, fault = top.locate_fault()

        # Get consumer loads from platform
        # Not always working so commenting it for now
        # if self.flag_load == 0:
        # ld = PowerData(self.msr_mrids_demand, message)
        # ld.demand()
        # self.flag_load = 1

        # Isolate and restore the fault
        if flag_fault == 1 and self.flag_res == 0:
            # Isolate the fault 
            opsw = []
            for f in fault:
                pr = OpenSw(f, self.LineData)
                op = pr.fault_isolation()
                opsw.append(op)
            opsw = [item for sublist in opsw for item in sublist]
            sw_o = SW_MRID(opsw, opsw, self.switches, self.LineData)
            op_mrid = sw_o.mapping_loc()

            # Fault Isolation in Platform
            for sw_mrid in op_mrid:
                self._open_diff.add_difference(sw_mrid, "Switch.open", 1, 0)
                msg = self._open_diff.get_message()
                self._gapps.send(self._publish_to_topic, json.dumps(msg))

            print('Forming the optimization problem.........')
            res = Restoration()
            op, cl, = res.res9500(self.LineData, self.DemandData, opsw)
            sw_oc = SW_MRID(op, cl, self.switches, self.LineData)
            op_mrid, cl_mrid = sw_oc.mapping_res()
            # print (op_mrid)
            # print(cl_mrid)   
            
            # Now reconfiguring the test case in Platform based on obtained MRIDs
            for sw_mrid in op_mrid:
                self._open_diff.add_difference(sw_mrid, "Switch.open", 1, 0)
                msg = self._open_diff.get_message()
                self._gapps.send(self._publish_to_topic, json.dumps(msg))  

            for sw_mrid in cl_mrid:
                self._open_diff.add_difference(sw_mrid, "Switch.open", 0, 1)
                msg = self._open_diff.get_message()
                self._gapps.send(self._publish_to_topic, json.dumps(msg))  
            self.flag_res = 1 
            print('Event #1 Successfully restored......')
            
def _main():
    _log.debug("Starting application")
    print("Application starting-------------------------------------------------------")
    global message_period
    parser = argparse.ArgumentParser()
    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("request",
                        help="Simulation Request")
    parser.add_argument("--message_period",
                        help="How often the sample app will send open/close capacitor message.",
                        default=DEFAULT_MESSAGE_PERIOD)
    opts = parser.parse_args()
    listening_to_topic = simulation_output_topic(opts.simulation_id)
    message_period = int(opts.message_period)
    sim_request = json.loads(opts.request.replace("\'",""))
    model_mrid = sim_request['power_system_config']['Line_name']
    print("\n \n The model running is IEEE 9500-node with MRID:", model_mrid)
    
    _log.debug("Model mrid is: {}".format(model_mrid))
    gapps = GridAPPSD(opts.simulation_id, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())
    topic = "goss.gridappsd.process.request.data.powergridmodel"

    # Run queries to get model information
    print('Get Model Information.....')   
    query = MODEL_EQ(gapps, model_mrid, topic)
    obj_msr_loadsw, obj_msr_demand = query.meas_mrids()
    switches = query.get_switches_mrids()
    

    # Load demand and lineparameters
    with open('Demand9500.json', 'r') as read_file:
        demand = json.load(read_file)
    with open('LineData.json', 'r') as read_file:
        line = json.load(read_file)
    # with open('Switches.json', 'r') as read_file:
    #     switches = json.load(read_file)

    print("Initialize.....")
    toggler = SwitchingActions(opts.simulation_id, gapps, switches, \
    obj_msr_loadsw, obj_msr_demand, demand, line)
    print("Now subscribing....")
    gapps.subscribe(listening_to_topic, toggler)
    while True:
        time.sleep(0.1)

if __name__ == "__main__":
    _main()
