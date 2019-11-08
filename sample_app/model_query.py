# -*- coding: utf-8 -*-
"""
Created on Wed Oct 23 11:17:12 2019

@author: spoudel
"""

class MODEL_EQ(object):
    """
    WSU Resilient Restoration. Mapping Switch MRIDs
    """
    def __init__(self, gapps, model_mrid, topic):
        self.gapps = gapps
        self.model_mrid = model_mrid
        self.topic = topic
        
    def get_switches_mrids(self):
        query = """
    PREFIX r:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX c:  <http://iec.ch/TC57/CIM100#>
    SELECT ?cimtype ?name ?bus1 ?bus2 ?id (group_concat(distinct ?phs;separator="") as ?phases) WHERE {
    SELECT ?cimtype ?name ?bus1 ?bus2 ?phs ?id WHERE {
    VALUES ?fdrid {"%s"}  # 9500 node
    VALUES ?cimraw {c:LoadBreakSwitch c:Recloser c:Breaker}
    ?fdr c:IdentifiedObject.mRID ?fdrid.
    ?s r:type ?cimraw.
    bind(strafter(str(?cimraw),"#") as ?cimtype)
    ?s c:Equipment.EquipmentContainer ?fdr.
    ?s c:IdentifiedObject.name ?name.
    ?s c:IdentifiedObject.mRID ?id.
    ?t1 c:Terminal.ConductingEquipment ?s.
    ?t1 c:ACDCTerminal.sequenceNumber "1".
    ?t1 c:Terminal.ConnectivityNode ?cn1. 
    ?cn1 c:IdentifiedObject.name ?bus1.
    ?t2 c:Terminal.ConductingEquipment ?s.
    ?t2 c:ACDCTerminal.sequenceNumber "2".
    ?t2 c:Terminal.ConnectivityNode ?cn2. 
    ?cn2 c:IdentifiedObject.name ?bus2
        OPTIONAL {?swp c:SwitchPhase.Switch ?s.
        ?swp c:SwitchPhase.phaseSide1 ?phsraw.
        bind(strafter(str(?phsraw),"SinglePhaseKind.") as ?phs) }
    } ORDER BY ?name ?phs
    }
    GROUP BY ?cimtype ?name ?bus1 ?bus2 ?id
    ORDER BY ?cimtype ?name
        """ % self.model_mrid
        results = self.gapps.query_data(query, timeout=60)
        results_obj = results['data']
        switches = []
        for p in results_obj['results']['bindings']:
            sw_mrid = p['id']['value']
            fr_to = [p['bus1']['value'].upper(), p['bus2']['value'].upper()]
            message = dict(name = p['name']['value'],
                        mrid = sw_mrid,
                        sw_con = fr_to)
            switches.append(message) 
        return switches

    def meas_mrids(self):

        # Get measurement MRIDS for LoadBreakSwitches
        message = {
        "modelId": self.model_mrid,
        "requestType": "QUERY_OBJECT_MEASUREMENTS",
        "resultFormat": "JSON",
        "objectType": "LoadBreakSwitch"}     
        obj_msr_loadsw = self.gapps.get_response(self.topic, message, timeout=180)   

        # Get measurement MRIDS for kW consumptions at each node
        message = {
            "modelId": self.model_mrid,
            "requestType": "QUERY_OBJECT_MEASUREMENTS",
            "resultFormat": "JSON",
            "objectType": "EnergyConsumer"}     
        obj_msr_demand = self.gapps.get_response(self.topic, message, timeout=180)
        print('check')
        return obj_msr_loadsw, obj_msr_demand
    

