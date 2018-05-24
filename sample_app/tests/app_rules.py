from durable.lang import *
import argparse
import datetime
import time
from collections import defaultdict
import stomp
import json
import math
import getpass

from gridappsd import GOSS
goss = GOSS()
goss.connect()

goss_log = 'goss.gridappsd.platform.log'
responseQueueTopic = '/temp-queue/response-queue'

debug=True

def run_rules(topic='input',port=5000, run_start = "2017-07-21 12:00:00", run_end = "2017-07-22 12:00:00"):
    #2017-07-21T18:00Z 2017-07-22T18:00Z
    print ("Start data {0} and end date {1}".format(run_start,run_end))


    def send_log_msg(msg):
        print ('Send log')
        logMsg['logMessage'] = msg
        now = datetime.datetime.now()
        logMsg['timestamp'] = now.strftime("%Y-%m-%d %H:%M:%S")
        t_now = datetime.datetime.utcnow()
        logMsg['timestamp'] = int(time.mktime(t_now.timetuple()) * 1000) + t_now.microsecond
        print logMsg['timestamp']
        logMsgStr = json.dumps(logMsg)
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

    testInput = ruleset(topic)

    shunt_dict = defaultdict(lambda: {'count':0})
    shunt_threshold = 2

    switch_dict = defaultdict(lambda: {'count':0})
    switch_threshold = 2

    base_kv = 12470 / 3

    with testInput:

        # Check a certain mrid's measurement Voltage P.U.
        # PowerTransformer_hvmv_sub_Voltage mrids
        transformer_voltages = {u'_11e4ede6-0dbf-494a-9950-e71058dfc599': u'PowerTransformer_hvmv_sub_Voltage_B',
                                u'_709c9f09-87ea-4027-969b-41050b6ef8fe': u'PowerTransformer_hvmv_sub_Voltage_A',
                                u'_f7412f91-4ae7-4adc-8442-47756decd6f8': u'PowerTransformer_hvmv_sub_Voltage_C'}

        @when_all(+m.message.measurements)
        def node_meas_check(c):
            # consequent
            measurements = c.m.message.measurements
            # print ("Magnitude check " + str(len(measurements)) + ' ' + measurements[0]['measurement_mrid'])
            for measurements_ in c.m.message.measurements:
                if measurements_['measurement_mrid'] in transformer_voltages.keys():
                    mag = measurements_['magnitude']
                    ang = measurements_['angle']
                    check_voltage(c, measurements_['measurement_mrid'], mag, ang, base_kv)

        def check_voltage(c, mrid, mag, ang, base_kv):
            c_temp = complex(mag, ang)
            volt_pu = (math.sqrt(c_temp.real ** 2 + c_temp.imag ** 2) / (base_kv)) / math.sqrt(3)
            print ("Magnitude check " + str(volt_pu))
            if volt_pu < .95 or volt_pu > 1.05:
                print (transformer_voltages[mrid] + " voltage p.u. out of threshold " + str(volt_pu) + " at " + c.m.message.timestamp)
                send_log_msg(transformer_voltages[mrid] + " voltage p.u. out of threshold " + str(volt_pu) + " at " + c.m.message.timestamp)

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
        m.message.forward_differences.allItems(item.attribute == 'ShuntCompensator.sections')))
        def shunt_change(c):
            # consequent
            # print ('Shunt' + str(c.m.message.reverse_differences[0]))
            for i,f in enumerate(c.m.message.reverse_differences):
                # print ('Count shunt changes: {0} '.format(f))
                c.post({
                        # 'shunt_object': c.m.message.difference_mrid,
                        'shunt_object': f['object'],
                        'action': f['attribute'],
                        'timestamp': c.m.message.timestamp})

        @when_all(+m.shunt_object)
        def count_shunt_object(c):
            shunt_dict[c.m.shunt_object]['count']+=1
            if shunt_dict[c.m.shunt_object]['count'] == shunt_threshold:
                print ('Shunt change threshold '+str(shunt_threshold)+' exceeded for shunt object ' + c.m.shunt_object)
                send_log_msg('Shunt change threshold '+str(shunt_threshold)+' exceeded for shunt object ' + c.m.shunt_object)


        @when_start
        def start(host):
            print('Topic', topic)
            if debug :
                # host.assert_fact(topic, {'mrid': 1, 'time':1})
                # host.assert_fact(topic, {'mrid': 1, 'time':2})
                # host.assert_fact(topic, {'mrid': 2, 'time':2})
                # host.assert_fact(topic, {'mrid': 1, 'time':3})
                #
                # host.post(topic, {'mrid': 1234, 'time': 1})
                # host.post(topic, {'mrid': 1234, 'time': 1})
                # host.post(topic, {'mrid': 1234, 'time': 1})

                # host.post(topic, {'shunt_object': '12345', 'time': 1})
                # host.post(topic, {'shunt_object': '12345', 'time': 1})
                meas1 = {
                    "simulation_id" : "12ae2345",
                    "message" : {
                        "timestamp" : "2018-01-08T13:27:00.000Z",
                        "measurements" : [{
                            "measurement_mrid" : "f98c9731-ca00-4372-bf41-48b5d39a0795",
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
                            "measurement_mrid" : "f98c9731-ca00-4372-bf41-48b5d39a0795",
                            "magnitude" : 4154.196028,
                            "angle" : -4422.093355
                        }]
                    }
                }
                host.post(topic, meas1)
                host.post(topic, meas2)
                diff = {"message": {"timestamp": "2018-05-21 21:05:01.964577+00:00", "reverse_differences": [
                    {"attribute": "ShuntCompensator.sections", "object": "_A5866105-A527-F682-C982-69807C0E088B",
                     "value": 0},
                    {"attribute": "ShuntCompensator.sections", "object": "_614B787E-649F-6FFE-EA99-9260674DA020",
                     "value": 0},
                    {"attribute": "ShuntCompensator.sections", "object": "_55A8A692-8286-60F4-3B55-E493AB7F5C14",
                     "value": 0},
                    {"attribute": "ShuntCompensator.sections", "object": "_A1395901-50BC-8B13-8CD6-F01B03CC8F65",
                     "value": 0},
                    {"attribute": "ShuntCompensator.sections", "object": "_890CCB37-5678-1B5D-9A6D-DE041407F004",
                     "value": 0},
                    {"attribute": "ShuntCompensator.sections", "object": "_D188697C-E7E2-2433-B115-B5F5066C157A",
                     "value": 0},
                    {"attribute": "ShuntCompensator.sections", "object": "_60028107-B79F-457E-B4F9-4FCCDD4725C7",
                     "value": 0},
                    {"attribute": "ShuntCompensator.sections", "object": "_44FBB17E-ADB8-F044-7182-7FB9F10D438C",
                     "value": 0},
                    {"attribute": "ShuntCompensator.sections", "object": "_04B2EB15-9590-4DB2-6983-A21D7973BC07",
                     "value": 0},
                    {"attribute": "ShuntCompensator.sections", "object": "_9EA4D055-B7EF-F289-8485-9159C1867059",
                     "value": 0}], "forward_differences": [
                    {"attribute": "ShuntCompensator.sections", "object": "_A5866105-A527-F682-C982-69807C0E088B",
                     "value": 1},
                    {"attribute": "ShuntCompensator.sections", "object": "_614B787E-649F-6FFE-EA99-9260674DA020",
                     "value": 1},
                    {"attribute": "ShuntCompensator.sections", "object": "_55A8A692-8286-60F4-3B55-E493AB7F5C14",
                     "value": 1},
                    {"attribute": "ShuntCompensator.sections", "object": "_A1395901-50BC-8B13-8CD6-F01B03CC8F65",
                     "value": 1},
                    {"attribute": "ShuntCompensator.sections", "object": "_890CCB37-5678-1B5D-9A6D-DE041407F004",
                     "value": 1},
                    {"attribute": "ShuntCompensator.sections", "object": "_D188697C-E7E2-2433-B115-B5F5066C157A",
                     "value": 1},
                    {"attribute": "ShuntCompensator.sections", "object": "_60028107-B79F-457E-B4F9-4FCCDD4725C7",
                     "value": 1},
                    {"attribute": "ShuntCompensator.sections", "object": "_44FBB17E-ADB8-F044-7182-7FB9F10D438C",
                     "value": 1},
                    {"attribute": "ShuntCompensator.sections", "object": "_04B2EB15-9590-4DB2-6983-A21D7973BC07",
                     "value": 1},
                    {"attribute": "ShuntCompensator.sections", "object": "_9EA4D055-B7EF-F289-8485-9159C1867059",
                     "value": 1}], "difference_mrid": "6ab63d3f-7f65-4dcf-a81f-f02b3cda5989"},
                                  "simulation_id": "1441855227"}
                host.post(topic, diff)

    if getpass.getuser() == 'root': # Docker check
        run_all([{'host': 'redis', 'port':6379}])
    else:
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
