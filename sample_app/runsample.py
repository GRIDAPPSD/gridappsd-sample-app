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

@author: Craig Allwardt
"""

__version__ = "0.0.8"

import argparse
import json
import logging
import time

from gridappsd import GridAPPSD, DifferenceBuilder
import gridappsd.topics as t


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('stomp.py').setLevel(logging.ERROR)
_log = logging.getLogger(__name__)

#TODO: remove simulation_id after updating GridAPPS-D API
simulation_id = 'field_data'

class Toggler(object):
    """ A simple class that handles publishing forward and reverse differences

    The object should be used as a callback from a GridAPPSD object so that the
    on_message function will get called each time a message from the simulator.  During
    the execution of on_meessage the `Toggler` object will publish a
    message to the simulation_input_topic with the forward and reverse difference specified.
    """

    def __init__(self, gridappsd_obj, object_list):
        """ Create a ``Toggler`` object

        This object should be used as a subscription callback from a ``GridAPPSD``
        object.  This class will toggle the objects passed to the constructor
        off and on for every message that is received on the ``field_output_topic``.

        Parameters
        ----------
        gridappsd_obj: GridAPPSD
            An instatiated object that is connected to the gridappsd message bus
            usually this should be the same object which subscribes, but that
            isn't required.
        object_list: list(str)
            A list of mrids to turn on/off
        """
        self._gapps = gridappsd_obj
        self._object_list = object_list
        self._last_toggle_on = False
        self._open_diff = DifferenceBuilder(simulation_id)
        self._close_diff = DifferenceBuilder(simulation_id)
        self._publish_to_topic = t.field_input_topic()
        for obj_mrid in object_list:
            _log.debug(f"Adding list: {obj_mrid}")
            self._open_diff.add_difference(obj_mrid, "Switch.open", 0, 1)
            self._close_diff.add_difference(obj_mrid, "Switch.open", 1, 0)

    def on_message(self, headers, message):
        """ Handle incoming messages on the field_output_topic

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

        if self._last_toggle_on:
            _log.debug("toggling off")
            msg = self._close_diff.get_message(epoch=message['message']['timestamp'])
            self._last_toggle_on = False
        else:
            _log.debug("toggling on")
            msg = self._open_diff.get_message(epoch=message['message']['timestamp'])
            self._last_toggle_on = True

        self._gapps.send(self._publish_to_topic, json.dumps(msg))

def get_object_mrids(gridappsd_obj, model_mrid):
    
    object_list = []
    response = gridappsd_obj.query_object_dictionary(model_mrid, object_type='LoadBreakSwitch')
    for objects in response['data']:
        object_list.append(objects['id'])
    return object_list


def _main():
    _log.debug("Starting application")
    
    '''parser = argparse.ArgumentParser()
    parser.add_argument("request",
                        help="Simulation Request")
    opts = parser.parse_args()
    
    sim_request = json.loads(opts.request.replace("\'",""))
    model_mrid = sim_request["power_system_config"]["Line_name"]'''
    
    model_mrid = '_C1C3E687-6FFD-C753-582B-632A27E28507'
    
    field_output_topic = t.field_output_topic()
    gapps = GridAPPSD()
    objects = get_object_mrids(gapps, model_mrid)
    toggler = Toggler(gapps, objects)
    gapps.subscribe(field_output_topic, toggler)
    while True:
        time.sleep(0.1)


if __name__ == "__main__":
    _main()
