import json
import sys
import stomp
import time
import os
import argparse

goss_output_topic = 'goss.gridappsd/fncs/output'
goss_simulation_status_topic = 'goss.gridappsd/simulation/status/'
goss_sim ="goss.gridappsd.process.request.simulation"
test_topic = 'goss.gridappsd.process.test'
test_topic = 'goss.gridappsd.test'

responseQueueTopic = '/temp-queue/response-queue'
goss_simulation_status_topic = '/topic/goss.gridappsd/simulation/status/'


gossConnection= None
isInitialized = None
simulationId = None

class GOSSStatusListener(object):
    def on_message(self, headers, msg):
        message = ''
        print('status ',msg)
        def on_error(self, headers, msg):
            print('simulation status error      ',msg)

class GOSSSimulationStartListener(object):
    def on_message(self, headers, msg):
        message = ''
        print('simulation start ', msg)
        global simulationId
        simulationId = int(msg)
        _registerWithGOSS('system','manager', msg,gossServer='localhost',stompPort='61613')
        def on_error(self, headers, msg):
            print('simulation start error     ',msg)

def _registerWithGOSS(username,password,simulationId,gossServer='localhost',
                      stompPort='61613'):
    '''Register with the GOSS server broker and return.

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
        '''
    if (gossServer == None or gossServer == ''
        or type(gossServer) != str):
        raise ValueError('gossServer must be a nonempty string.\n'
                         + 'gossServer = {0}'.format(gossServer))
    if (stompPort == None or stompPort == ''
        or type(stompPort) != str):
        raise ValueError('stompPort must be a nonempty string.\n'
                         + 'stompPort = {0}'.format(stompPort))
    gossConnection = stomp.Connection12([(gossServer, stompPort)])
    gossConnection.start()
    gossConnection.connect(username,password)
    gossConnection.set_listener('GOSSStatusListener', GOSSStatusListener())
    gossConnection.subscribe(goss_output_topic,1)

def _startTest(username,password,gossServer='localhost',stompPort='61613', simulationID=1234, rulePort=5000, topic="input"):
    loc = os.path.realpath(__file__)
    loc =  os.path.dirname(loc)
    # loc =  os.path.dirname(os.path.dirname(os.path.dirname(loc)))
    print (loc)
    testCfg = {"testConfigPath":loc+"/SampleTestConfig.json",
            "testScriptPath":loc+"/SampleTestScript.json",
            "simulationID": 1234,
            "rulePort": 5000,
            "topic":"input",
            "expectedResult":loc+"/expected_result_series.json"
            }

    simCfgOld ='''{"power_system_config":{"SubGeographicalRegion_name":"ieee8500nodecktassets_SubRegion","GeographicalRegion_name":"ieee8500nodecktassets_Region","Line_name":"ieee8500"},"simulation_config":{"power_flow_solver_method":"NR","duration":120,"simulation_name":"ieee8500","simulator":"GridLAB-D","start_time":"2009-07-21 00:00:00","timestep_frequency":1000,"timestep_increment":1000,"simulation_output":{"output_objects":[{"name":"rcon_FEEDER_REG","properties":["connect_type","Control","control_level","PT_phase","band_center","band_width","dwell_time","raise_taps","lower_taps","regulation"]},{"name":"rcon_VREG2","properties":["connect_type","Control","control_level","PT_phase","band_center","band_width","dwell_time","raise_taps","lower_taps","regulation"]},{"name":"rcon_VREG3","properties":["connect_type","Control","control_level","PT_phase","band_center","band_width","dwell_time","raise_taps","lower_taps","regulation"]},{"name":"rcon_VREG4","properties":["connect_type","Control","control_level","PT_phase","band_center","band_width","dwell_time","raise_taps","lower_taps","regulation"]},{"name":"reg_FEEDER_REG","properties":["configuration","phases","to","tap_A","tap_B","tap_C"]},{"name":"reg_VREG2","properties":["configuration","phases","to","tap_A","tap_B","tap_C"]},{"name":"reg_VREG3","properties":["configuration","phases","to","tap_A","tap_B","tap_C"]},{"name":"reg_VREG4","properties":["configuration","phases","to","tap_A","tap_B","tap_C"]},{"name":"cap_capbank0a","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_A","dwell_time","switchA"]},{"name":"cap_capbank1a","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_A","dwell_time","switchA"]},{"name":"cap_capbank2a","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_A","dwell_time","switchA"]},{"name":"cap_capbank0b","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_B","dwell_time","switchB"]},{"name":"cap_capbank1b","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_B","dwell_time","switchB"]},{"name":"cap_capbank2b","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_B","dwell_time","switchB"]},{"name":"cap_capbank0c","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_C","dwell_time","switchC"]},{"name":"cap_capbank1c","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_C","dwell_time","switchC"]},{"name":"cap_capbank2c","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_C","dwell_time","switchC"]},{"name":"cap_capbank3","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_A","capacitor_B","capacitor_C","dwell_time","switchA","switchB","switchC"]},{"name":"xf_hvmv_sub","properties":["power_in_A","power_in_B","power_in_C"]},{"name":"l2955047","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"l2673313","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"l3160107","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"l2876814","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"l3254238","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"m1047574","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"_hvmv_sub_lsb","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"190-8593","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"190-8581","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"190-7361","properties":["voltage_A","voltage_B","voltage_C"]}]},"model_creation_config":{"load_scaling_factor":1.0,"triplex":"y","encoding":"u","system_frequency":60,"voltage_multiplier":1.0,"power_unit_conversion":1.0,"unique_names":"y","schedule_name":"ieeezipload","z_fraction":0.0,"i_fraction":1.0,"p_fraction":0.0},"simulation_broker_port":51233,"simulation_broker_location":"127.0.0.1"},"application_config":{"applications":[{"name":"sample_app","config_string":"{}"}]}}'''

    simCfg =''' {"power_system_config":{"GeographicalRegion_name":"_24809814-4EC6-29D2-B509-7F8BFB646437","SubGeographicalRegion_name":"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224","Line_name":"_C1C3E687-6FFD-C753-582B-632A27E28507"},"simulation_config":{"start_time":"2009-07-21 00:00:00","duration":"120","simulator":"GridLAB-D","timestep_frequency":"1000","timestep_increment":"1000","simulation_name":"ieee8500","power_flow_solver_method":"NR","simulation_output":{"output_objects":[{"name":"rcon_FEEDER_REG","properties":["connect_type","Control","control_level","PT_phase","band_center","band_width","dwell_time","raise_taps","lower_taps","regulation"]},{"name":"rcon_VREG2","properties":["connect_type","Control","control_level","PT_phase","band_center","band_width","dwell_time","raise_taps","lower_taps","regulation"]},{"name":"rcon_VREG3","properties":["connect_type","Control","control_level","PT_phase","band_center","band_width","dwell_time","raise_taps","lower_taps","regulation"]},{"name":"rcon_VREG4","properties":["connect_type","Control","control_level","PT_phase","band_center","band_width","dwell_time","raise_taps","lower_taps","regulation"]},{"name":"reg_FEEDER_REG","properties":["configuration","phases","to","tap_A","tap_B","tap_C"]},{"name":"reg_VREG2","properties":["configuration","phases","to","tap_A","tap_B","tap_C"]},{"name":"reg_VREG3","properties":["configuration","phases","to","tap_A","tap_B","tap_C"]},{"name":"reg_VREG4","properties":["configuration","phases","to","tap_A","tap_B","tap_C"]},{"name":"cap_capbank0a","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_A","dwell_time","switchA"]},{"name":"cap_capbank1a","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_A","dwell_time","switchA"]},{"name":"cap_capbank2a","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_A","dwell_time","switchA"]},{"name":"cap_capbank0b","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_B","dwell_time","switchB"]},{"name":"cap_capbank1b","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_B","dwell_time","switchB"]},{"name":"cap_capbank2b","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_B","dwell_time","switchB"]},{"name":"cap_capbank0c","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_C","dwell_time","switchC"]},{"name":"cap_capbank1c","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_C","dwell_time","switchC"]},{"name":"cap_capbank2c","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_C","dwell_time","switchC"]},{"name":"cap_capbank3","properties":["phases","pt_phase","phases_connected","control","control_level","capacitor_A","capacitor_B","capacitor_C","dwell_time","switchA","switchB","switchC"]},{"name":"xf_hvmv_sub","properties":["power_in_A","power_in_B","power_in_C"]},{"name":"l2955047","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"l2673313","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"l3160107","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"l2876814","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"l3254238","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"m1047574","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"_hvmv_sub_lsb","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"190-8593","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"190-8581","properties":["voltage_A","voltage_B","voltage_C"]},{"name":"190-7361","properties":["voltage_A","voltage_B","voltage_C"]}]},"model_creation_config":{"load_scaling_factor":"1","schedule_name":"ieeezipload","z_fraction":"0","i_fraction":"1","p_fraction":"0"}},"application_config":{"applications":[{"name":"sample_app","config_string":"{}"}]}}'''

    # simCfg='''{"power_system_configuration":"ieee8500","simulation_configuration":"ieee8500","run_start":"Jul 21, 2017 12:00:00 PM","run_end":"Jul 22, 2017 12:00:00 PM","region_name":"ieee8500_Region","subregion_name":"ieee8500_SubRegion","line_name":"ieee8500","logging":true,"logging_options":{"log":"true"},"initial_conditions":{},"default_values":{}}'''
    simCfg=''' {\"power_system_config\":{\"SubGeographicalRegion_name\":\"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224\",\"GeographicalRegion_name\":\"_24809814-4EC6-29D2-B509-7F8BFB646437\",\"Line_name\":\"_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3\"},\"simulation_config\":{\"power_flow_solver_method\":\"NR\",\"duration\":120,\"simulation_name\":\"ieee8500\",\"simulator\":\"GridLAB-D\",\"start_time\":\"2009-07-21 00:00:00\",\"timestep_frequency\":1000,\"timestep_increment\":1000,\"simulation_output\":{\"output_objects\":[{\"name\":\"rcon_FEEDER_REG\",\"properties\":[\"connect_type\",\"Control\",\"control_level\",\"PT_phase\",\"band_center\",\"band_width\",\"dwell_time\",\"raise_taps\",\"lower_taps\",\"regulation\"]},{\"name\":\"rcon_VREG2\",\"properties\":[\"connect_type\",\"Control\",\"control_level\",\"PT_phase\",\"band_center\",\"band_width\",\"dwell_time\",\"raise_taps\",\"lower_taps\",\"regulation\"]},{\"name\":\"rcon_VREG3\",\"properties\":[\"connect_type\",\"Control\",\"control_level\",\"PT_phase\",\"band_center\",\"band_width\",\"dwell_time\",\"raise_taps\",\"lower_taps\",\"regulation\"]},{\"name\":\"rcon_VREG4\",\"properties\":[\"connect_type\",\"Control\",\"control_level\",\"PT_phase\",\"band_center\",\"band_width\",\"dwell_time\",\"raise_taps\",\"lower_taps\",\"regulation\"]},{\"name\":\"reg_FEEDER_REG\",\"properties\":[\"configuration\",\"phases\",\"to\",\"tap_A\",\"tap_B\",\"tap_C\"]},{\"name\":\"reg_VREG2\",\"properties\":[\"configuration\",\"phases\",\"to\",\"tap_A\",\"tap_B\",\"tap_C\"]},{\"name\":\"reg_VREG3\",\"properties\":[\"configuration\",\"phases\",\"to\",\"tap_A\",\"tap_B\",\"tap_C\"]},{\"name\":\"reg_VREG4\",\"properties\":[\"configuration\",\"phases\",\"to\",\"tap_A\",\"tap_B\",\"tap_C\"]},{\"name\":\"cap_capbank0a\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_A\",\"dwell_time\",\"switchA\"]},{\"name\":\"cap_capbank1a\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_A\",\"dwell_time\",\"switchA\"]},{\"name\":\"cap_capbank2a\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_A\",\"dwell_time\",\"switchA\"]},{\"name\":\"cap_capbank0b\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_B\",\"dwell_time\",\"switchB\"]},{\"name\":\"cap_capbank1b\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_B\",\"dwell_time\",\"switchB\"]},{\"name\":\"cap_capbank2b\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_B\",\"dwell_time\",\"switchB\"]},{\"name\":\"cap_capbank0c\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_C\",\"dwell_time\",\"switchC\"]},{\"name\":\"cap_capbank1c\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_C\",\"dwell_time\",\"switchC\"]},{\"name\":\"cap_capbank2c\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_C\",\"dwell_time\",\"switchC\"]},{\"name\":\"cap_capbank3\",\"properties\":[\"phases\",\"pt_phase\",\"phases_connected\",\"control\",\"control_level\",\"capacitor_A\",\"capacitor_B\",\"capacitor_C\",\"dwell_time\",\"switchA\",\"switchB\",\"switchC\"]},{\"name\":\"xf_hvmv_sub\",\"properties\":[\"power_in_A\",\"power_in_B\",\"power_in_C\"]},{\"name\":\"l2955047\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]},{\"name\":\"l2673313\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]},{\"name\":\"l3160107\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]},{\"name\":\"l2876814\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]},{\"name\":\"l3254238\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]},{\"name\":\"m1047574\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]},{\"name\":\"_hvmv_sub_lsb\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]},{\"name\":\"190-8593\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]},{\"name\":\"190-8581\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]},{\"name\":\"190-7361\",\"properties\":[\"voltage_A\",\"voltage_B\",\"voltage_C\"]}]},\"model_creation_config\":{\"load_scaling_factor\":1.0,\"triplex\":\"y\",\"encoding\":\"u\",\"system_frequency\":60,\"voltage_multiplier\":1.0,\"power_unit_conversion\":1.0,\"unique_names\":\"y\",\"schedule_name\":\"ieeezipload\",\"z_fraction\":0.0,\"i_fraction\":1.0,\"p_fraction\":0.0},\"simulation_broker_port\":5570,\"simulation_broker_location\":\"127.0.0.1\"},\"application_config\":{\"applications\":[{\"name\":\"sample_app\",\"config_string\":\"{}\"}]}}'''

    if (gossServer == None or gossServer == ''
        or type(gossServer) != str):
        raise ValueError(
                         'gossServer must be a nonempty string.\n'
                         + 'gossServer = {0}'.format(gossServer))
    if (stompPort == None or stompPort == ''
        or type(stompPort) != str):
        raise ValueError(
                         'stompPort must be a nonempty string.\n'
                         + 'stompPort = {0}'.format(stompPort))
    gossConnection = stomp.Connection12([(gossServer, stompPort)])
    gossConnection.set_listener('GOSSSimulationStartListener',GOSSSimulationStartListener())

    gossConnection.start()
    gossConnection.connect(username, password, wait=True)
    # gossConnection.subscribe(destination='/queue/reply',id=2)
    gossConnection.subscribe(destination=responseQueueTopic,id=2)


    gossConnection.send(body=simCfg, destination=goss_sim, headers={'reply-to': responseQueueTopic})
    print('sent simulation request')
    time.sleep(1)
    testCfg['simulationID']=simulationId
    testCfg = json.dumps(testCfg)
    print(testCfg)
    # gossConnection.send(body=testCfg, destination=test_topic, headers={'reply-to': '/queue/reply'})
    gossConnection.send(body=testCfg, destination=test_topic, headers={'reply-to': responseQueueTopic})

    time.sleep(1)
    print('sent test request')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t","--topic", type=str, help="topic, the default is input", default="input", required=False)
    parser.add_argument("-p","--port", type=int, help="port number, the default is 5000", default=5000, required=False)
    parser.add_argument("-i", "--id", type=int, help="simulation id", required=False)
    parser.add_argument("--start_date", type=str, help="Simulation start date", default="2017-07-21 12:00:00", required=False)
    parser.add_argument("--end_date", type=str, help="Simulation end date" , default="2017-07-22 12:00:00", required=False)
    # parser.add_argument('-o', '--options', type=str, default='{}')
    args = parser.parse_args()


    _startTest('system','manager',gossServer='127.0.0.1',stompPort='61613', simulationID=args.id, rulePort=args.port, topic=args.topic)
