# -*- coding: utf-8 -*-
"""
Created on Wed Oct 23 11:17:12 2019

@author: spoudel
"""

class SW_MRID(object):
    """
    WSU Resilient Restoration. Mapping Switch MRIDs
    """
    def __init__(self, op, cl, switches, LineData):
        self.op = op
        self.cl = cl
        self.switches = switches
        self.LineData = LineData
        
    def mapping_res(self):
        op_mrid = []
        cl_mrid = []
        for l in self.LineData:
            if l['index'] in self.op:
                edge = set([l['from_br'], l['to_br']])
                for s in self.switches:
                    check = set(s['sw_con'])
                    if check == edge:
                        op_mrid.append(s['mrid'])
                        
            if l['index'] in self.cl:
                edge = set([l['from_br'], l['to_br']])
                for s in self.switches:
                    check = set(s['sw_con'])
                    if check == edge:
                        cl_mrid.append(s['mrid'])
        return op_mrid, cl_mrid
    
    def mapping_loc(self):
        op_mrid = []
        for l in self.LineData:
            if l['index'] in self.op:
                edge = set([l['from_br'], l['to_br']])
                for s in self.switches:
                    check = set(s['sw_con'])
                    if check == edge:
                        op_mrid.append(s['mrid'])
        return op_mrid