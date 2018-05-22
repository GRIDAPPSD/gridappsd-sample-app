import requests
import logging
import os
import signal
import sys
import time
import traceback

import json
import yaml
from gridappsd import GOSS

logger_name = "goss-forward"
user_home = os.path.expanduser("~")
logger_location = os.path.join(user_home, "var/log/" + logger_name + ".log")

if not os.path.exists(os.path.dirname(logger_location)):
    os.makedirs(os.path.dirname(logger_location))

read_topic = '/topic/goss.gridappsd.simulation.output.'
simulation_input_topic = '/topic/goss.gridappsd.simulation.input.'

goss_connection = None
is_initialized = False
# Number
simulation_id = None

hdlr = logging.FileHandler(logger_location)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)

logger = logging.getLogger(logger_name)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)


# Created in the bottom of the script before the instantiation of the
# GossListener.


class GOSSListener(object):
    def __init__(self, t0):
        self.t0 = t0
        self.expected = "expected_result_series.json"
        self.raw_str_file = "expected_str.json"
        self.raw_str_data = 'None'
        self.data = {"expected_outputs": {}}
        self.msgCount = 0

    def on_message(self, headers, msg):

        try:
            self.t0 += 1
            logger.debug('received message ' + str(msg[:200]))

            json_msg = yaml.safe_load(str(msg))

            # print str(msg)
            if 'output' not in json_msg or json_msg['output'] == None:
                print 'Output is none'
                return
            # output = yaml.safe_load(json_msg['output'])
            output = json_msg['output']
            # Ignore null output data or . (Assumes initializing)
            if output is None or 'message' not in output:
                return

            temp_meas = output['message']['measurements']
            len2 = len(output['message']['measurements'])
            start = 0
            chunk_size = 500
            for end in range(chunk_size, len2 - 1, chunk_size):
                output['message']['measurements'] = temp_meas[start:end]
                r = requests.post("http://localhost:5000/input/events", json=output)
                start = end
            # print(start, len2 - 1)
            output['message']['measurements'] = temp_meas[start: len2 - 1]
            r = requests.post("http://localhost:5000/input/events", json=output)

            print ('Forward ' + str(output)[:500])
            print r.status_code
            print r.headers

            self.raw_str_data = str(msg)
            self.save()
            # self.data["expected_outputs"][str(self.msgCount)] = output
            # self.msgCount+=1

            print("the output is: {}".format(str(output)[:500]))

        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            logger.error(type(e))
            logger.error(e.args)
            logger.error('Error in command ' + str(e))

    def on_error(self, headers, message):
        logger.error('Error in goss listener ' + str(message))

    def on_disconnected(self):
        logger.error('Disconnected')

    def save(self):
        with open(self.expected, 'w') as outfile:
            json.dump(self.data, outfile, sort_keys = True, indent = 2)

        with open(self.raw_str_file, 'w') as outfile:
            json.dump(self.raw_str_data, outfile, sort_keys=True, indent=2)

class GOSSListenerInput(object):
    def __init__(self, t0):
        self.t0 = t0

    def on_message(self, headers, msg):
        try:
            logger.debug('received message input ' + str(msg[:200]))

            json_msg = yaml.safe_load(str(msg))

            # print str(msg)
            if 'input' not in json_msg or json_msg['input'] == None:
                print 'input is none'
                return

            output = json_msg['input']
            # Ignore null output data or . (Assumes initializing)
            if output is None or 'message' not in output:
                return

            r = requests.post("http://localhost:5000/input/events", json=output)
            print ('Forward ' + str(output)[:500])
            print r.status_code
            print r.headers

            print("the input is: {}".format(str(output)[:500]))

        except Exception as e:
            traceback.print_exc(file=sys.stdout)
            logger.error(type(e))
            logger.error(e.args)
            logger.error('Error in command ' + str(e))

goss_listener = GOSSListener(2)

goss_listener_input = GOSSListenerInput(2)

def _keep_alive():
    while 1:
        time.sleep(0.1)

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    goss_listener.save()
    sys.exit(0)

def _register_with_goss(username, password, gossServer='localhost',
                      stompPort='61613', simID=''):
    """Register with the GOSS server broker and return.

    Function arguments:
        gossServer -- Type: string. Description: The ip location
        for the GOSS server. It must not be an empty string.
            Default: 'localhost'.
        stompPort -- Type: string. Description: The port for Stomp
        protocol for the GOSS server. It must not be an empty string.
            Default: '61613'.
        username -- Type: string. Description: User name for GOSS connection.
        password -- Type: string. Description: Password for GOSS connection.

    Function returns:
        None.
    Function exceptions:
        RuntimeError()
    """
    goss = GOSS()
    goss.connect()
    print ('GOSS connected ' + str(goss.connected))
    goss.subscribe(read_topic+simID, goss_listener)
    goss.subscribe(simulation_input_topic+simID,goss_listener_input)

def forward_sim_ouput(simID):
    _register_with_goss('system','manager',gossServer='127.0.0.1',stompPort='61613',simID=simID)
    signal.signal(signal.SIGINT, signal_handler)
    _keep_alive()

if __name__ == "__main__":
    import argparse

#    parser = argparse.ArgumentParser(version=__version__)
#    parser.add_argument("simulationId",
#                        help="Simulation id to use for responses on the message bus.")

    logger.debug("Waiting for ")

    # Connect and listen to the message bus for content.
    # The port should be cast to string because that makes the opening socket easier.
    _register_with_goss('system','manager',gossServer='127.0.0.1',stompPort='61613')
    #_register_with_goss(opts.user, opts.password, opts.address, str(opts.port))

    signal.signal(signal.SIGINT, signal_handler)

    # Sleep until notified of new data.
    _keep_alive()
