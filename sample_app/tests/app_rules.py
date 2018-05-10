from durable.lang import *
import argparse
import datetime
import time
from collections import defaultdict
import stomp
import json
import math

from gridappsd import GOSS
goss = GOSS()
goss.connect()

goss_log = 'goss.gridappsd.platform.log'
responseQueueTopic = '/temp-queue/response-queue'

def run_rules(topic='input',port=5000, run_start = "2017-07-21 12:00:00", run_end = "2017-07-22 12:00:00"):
    #2017-07-21T18:00Z 2017-07-22T18:00Z
    print ("Start data {0} and end date {1}".format(run_start,run_end))
    gossServer = 'localhost'
    stompPort = '61613'
    username = 'system'
    password = 'manager'
    if (gossServer == None or gossServer == '' or type(gossServer) != str):
        raise ValueError(
                         'gossServer must be a nonempty string.\n'
                         + 'gossServer = {0}'.format(gossServer))
    if (stompPort == None or stompPort == ''
        or type(stompPort) != str):
        raise ValueError(
                         'stompPort must be a nonempty string.\n'
                         + 'stompPort = {0}'.format(stompPort))
    gossConnection = stomp.Connection12([(gossServer, stompPort)])
    try:
        gossConnection.start()
        gossConnection.connect(username, password, wait=True)
    except:
        print ("Not connected to GOSS - messages will not be logged on the platform")


    def send_log_msg(msg):
        print ('Send log')
        logMsg['logMessage'] = msg
        now = datetime.datetime.now()
        logMsg['timestamp'] = now.strftime("%Y-%m-%d %H:%M:%S")
        t_now = datetime.datetime.utcnow()
        logMsg['timestamp'] = int(time.mktime(t_now.timetuple()) * 1000) + t_now.microsecond
        print logMsg['timestamp']
        logMsgStr = json.dumps(logMsg)
        print gossConnection.send(body=logMsgStr, destination=goss_log,
                            headers={'reply-to':responseQueueTopic})
        goss.send(body=logMsgStr, destination=goss_log)

    logMsg = {
        # 'id': 401,
        'source': 'rule',
        'processId': 'simple_app.rule',
        'timestamp': '2018-05-09 00:00:00',
        'logLevel': 'INFO',
        'logMessage': 'template msg',
        'processStatus': 'ERROR',
        # 'username': username,
        'storeToDb': True}

    # status_message = {
    #     "source": os.path.basename(__file__),
    #     "processId": str(simulation_id),
    #     "timestamp": int(time.mktime(t_now.timetuple()) * 1000) + t_now.microsecond,
    #     "processStatus": status,
    #     "logMessage": str(message),
    #     "logLevel": log_level,
    #     "storeToDb": True}


    testInput = ruleset(topic)

    shunt_dict = defaultdict(lambda: {'count':0})
    shunt_threshold = 1

    switch_dict = defaultdict(lambda: {'count':0})
    switch_threshold = 1

    base_kv = 12470 / 3

    with testInput:
        ## Get start and end from TestConfig

        # Check a certain mrid's measurement Voltage P.U.
        #u'PowerTransformer_hvmv_sub_Voltage
        transformer_voltages = ['b74b5d31-158e-49a7-8f92-3042598cfd66',
                             '95ca5999-6b99-49cd-b6bb-9468cfd66680',
                             '6ac54b7c-3eff-4e0c-8a84-53ceedeb5188']
        transformer_voltages = {u'6ac54b7c-3eff-4e0c-8a84-53ceedeb5188': u'PowerTransformer_hvmv_sub_Voltage_C',
         u'95ca5999-6b99-49cd-b6bb-9468cfd66680': u'PowerTransformer_hvmv_sub_Voltage_B',
         u'b74b5d31-158e-49a7-8f92-3042598cfd66': u'PowerTransformer_hvmv_sub_Voltage_A'}

        @when_all(+m.message.measurements)
        def node_meas_check3(c):
            # consequent
            measurements = c.m.message.measurements
            print ("Magnitude check " + str(len(measurements)) + ' ' + measurements[0]['measurement_mrid'])
            for measurements_ in c.m.message.measurements:
                if measurements_['measurement_mrid'] in transformer_voltages.keys():
                    mag = measurements_['magnitude']
                    ang = measurements_['angle']
                    check_voltage(c, measurements_['measurement_mrid'], mag, ang, base_kv)

        # @when_all(+m.message.measurement.magnitude)
        # def node_meas_check1(c):
        #     # consequent
        #     print ("Magnitude check")
        #     if c.m.message.measurement.measurement_mrid in transformer_voltages:
        #         check_voltage(c, base_kv)
        #
        # @when_all(m.message.measurement.measurement_mrid == "123a456b-789c-012d-345e-678f901a234b")
        # def node_meas_check2(c):
        #     # consequent
        #     check_voltage(c, base_kv)

        def check_voltage(c, mrid, mag, ang, base_kv):
            c_temp = complex(mag, ang)
            volt_pu = (math.sqrt(c_temp.real ** 2 + c_temp.imag ** 2) / (base_kv)) / math.sqrt(3)
            print ("Magnitude check " + str(volt_pu))
            if volt_pu < .95 or volt_pu > 1.05:
                print (transformer_voltages[mrid] + " voltage p.u. out of threshold " + str(volt_pu) + " at " + c.m.message.timestamp)
                send_log_msg(transformer_voltages[mrid] + " voltage p.u. out of threshold " + str(volt_pu) + " at " + c.m.message.timestamp)

        # def check_voltage(c, base_kv):
        #     mag = c.m.message.measurement.magnitude
        #     ang = c.m.message.measurement.angle
        #     c_temp = complex(mag, ang)
        #     volt_pu = (math.sqrt(c_temp.real ** 2 + c_temp.imag ** 2) / (base_kv)) / math.sqrt(3)
        #     if volt_pu < .95 or volt_pu > 1.05:
        #         print "Voltage out of threshold " + str(volt_pu) + " at " + c.m.message.timestamp
        #         send_log_msg("Voltage out of threshold " + str(volt_pu) + " at " + c.m.message.timestamp)

        # A Reverse and a Forward difference is a state change.
        @when_all((m.message.reverse_difference.attribute == 'Switch.open') & (
        m.message.reverse_difference.attribute == 'Switch.open'))
        def switch_open(c):
            # consequent
            c.post({'mrid': c.m.message.difference_mrid,
                    'action': c.m.message.reverse_difference.attribute,
                    'timestamp': c.m.message.timestamp})

        @when_all(+m.mrid)
        def count_switch(c):
            switch_dict[c.m.mrid]['count']+=1
            if switch_dict[c.m.mrid]['count'] == switch_threshold:
                print ("For Posting: 3 changes at different times at the same switch.")
                send_log_msg(str(switch_threshold) + " changes at different times at the same switch.")


        # A Reverse and a Forward difference is a state change.
        @when_all((m.message.reverse_differences.allItems(item.attribute == 'ShuntCompensator.sections')) & (
        m.message.reverse_differences.allItems(item.attribute == 'ShuntCompensator.sections')))
        def shunt_change(c):
            # consequent
            # print ('Shunt' + c.m.message.reverse_differences[0])
            for i,f in enumerate(c.m.message.reverse_differences):
                print ('Count shunt changes: {0} '.format(f))
                c.post({
                        # 'shunt_object': c.m.message.difference_mrid,
                        'shunt_object' : f['object'],
                        'action': f['attribute'],
                        'timestamp': c.m.message.timestamp})

        @when_all(+m.shunt_object)
        def count_shunt_object(c):
            # print (c)
            shunt_dict[c.m.shunt_object]['count']+=1
            if shunt_dict[c.m.shunt_object]['count'] == shunt_threshold:
                print ('Shunt change threshold exceeded for shunt object ' + c.m.shunt_object)
                send_log_msg('Shunt change threshold exceeded for shunt object ' + c.m.shunt_object)


        @when_start
        def start(host):
            print('Topic', topic)

            host.assert_fact(topic, {'mrid': 1, 'time':1})
            host.assert_fact(topic, {'mrid': 1, 'time':2})
            host.assert_fact(topic, {'mrid': 2, 'time':2})
            host.assert_fact(topic, {'mrid': 1, 'time':3})

            host.post(topic, {'mrid': 1234, 'time': 1})
            host.post(topic, {'mrid': 1234, 'time': 1})
            host.post(topic, {'mrid': 1234, 'time': 1})
            meas1 = {
                "simulation_id" : "12ae2345",
                "message" : {
                    "timestamp" : "2018-01-08T13:27:00.000Z",
                    "measurements" : [{
                        "measurement_mrid" : "b74b5d31-158e-49a7-8f92-3042598cfd66",
                        "magnitude" : 1960.512425,
                        "angle" : 6912.904192
                    }]
                }
            }
            meas2 = {
                "simulation_id" : "12ae2345",
                "message" : {
                    "timestamp" : "2018-01-08T13:27:00.000Z",
                    "measurements" : [{
                        "measurement_mrid" : "b74b5d31-158e-49a7-8f92-3042598cfd66",
                        "magnitude" : 4154.196028,
                        "angle" : -4422.093355
                    }]
                }
            }
            host.post(topic, meas1)
            host.post(topic, meas2)

            # host.post(topic, {"simulation_id": "12ae2345", "message": {"timestamp": "2018-01-08T13:27:00.000Z",
            #                                                      "difference_mrid": "123a456b-789c-012d-345e-678f901a235c",
            #                                                      "reverse_differences": [
            #                                                          {"object": "61A547FB-9F68-5635-BB4C-F7F537FD824E",
            #                                                           "attribute": "ShuntCompensator.sections",
            #                                                           "value": "1"},
            #                                                          {"object": "E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA",
            #                                                           "attribute": "ShuntCompensator.sections",
            #                                                           "value": "0"}], "forward_differences": [
            # {"object": "61A547FB-9F68-5635-BB4C-F7F537FD824E", "attribute": "ShuntCompensator.sections", "value": "0"},
            # {"object": "E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA", "attribute": "ShuntCompensator.sections",
            #  "value": "1"}]}})
            # host.post(topic, {"simulation_id": "12ae2345", "message": {"timestamp": "2018-01-08T13:27:00.000Z",
            #                                                      "difference_mrid": "123a456b-789c-012d-345e-678f901a235c",
            #                                                      "reverse_differences": [
            #                                                          {"object": "61A547FB-9F68-5635-BB4C-F7F537FD824E",
            #                                                           "attribute": "ShuntCompensator.sections",
            #                                                           "value": "1"},
            #                                                          {"object": "E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA",
            #                                                           "attribute": "ShuntCompensator.sections",
            #                                                           "value": "0"}], "forward_differences": [
            # {"object": "61A547FB-9F68-5635-BB4C-F7F537FD824E", "attribute": "ShuntCompensator.sections", "value": "0"},
            # {"object": "E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA", "attribute": "ShuntCompensator.sections",
            #  "value": "1"}]}})
            # host.post(topic, {"simulation_id": "12ae2345", "message": {"timestamp": "2018-01-08T13:27:00.000Z",
            #                                                      "difference_mrid": "123a456b-789c-012d-345e-678f901a235c",
            #                                                      "reverse_differences": [
            #                                                          {"object": "61A547FB-9F68-5635-BB4C-F7F537FD824E",
            #                                                           "attribute": "ShuntCompensator.sections",
            #                                                           "value": "1"},
            #                                                          {"object": "E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA",
            #                                                           "attribute": "ShuntCompensator.sections",
            #                                                           "value": "0"}], "forward_differences": [
            # {"object": "61A547FB-9F68-5635-BB4C-F7F537FD824E", "attribute": "ShuntCompensator.sections", "value": "0"},
            # {"object": "E3CA4CD4-B0D4-9A83-3E2F-18AC5F1B55BA", "attribute": "ShuntCompensator.sections",
            #  "value": "1"}]}})

    run_all(port=port)

if __name__ == '__main__':
    x = '{"run_start" : "2017-07-21 12:00:00", "run_end" : "2017-07-22 12:00:00"}'
    #--topic input --port 5011 '{"run_start" : "2017-07-21 12:00:00", "run_end" : "2017-07-22 12:00:00"}'
    parser = argparse.ArgumentParser()
    parser.add_argument("-t","--topic", type=str, help="topic, the default is input", default="input")
    parser.add_argument("-p","--port", type=int, help="port number, the default is 5000", default=5000)
    parser.add_argument("-i", "--id", type=int, help="simulation id")
    parser.add_argument("--start_date", type=str, help="Simulation start date", default="2017-07-21 12:00:00", required=False)
    parser.add_argument("--end_date", type=str, help="Simulation end date" , default="2017-07-22 12:00:00", required=False)
    # parser.add_argument('-o', '--options', type=str, default='{}')
    args = parser.parse_args()
    # options_dict = json.loads(args.options)

    run_rules(topic=args.topic, port=args.port, run_start=args.start_date, run_end=args.end_date)
