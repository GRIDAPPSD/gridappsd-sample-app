from gridappsd import GridAPPSD, topics as t
from gridappsd.simulation import Simulation
from sim_class import SimulationClass
import time

import os
os.environ['GRIDAPPSD_USER'] = 'tutorial_user'
os.environ['GRIDAPPSD_PASSWORD'] = '12345!'
os.environ['GRIDAPPSD_ADDRESS'] = 'localhost'
os.environ['GRIDAPPSD_PORT'] = '61613'

def main():
    gapps = GridAPPSD()
    assert gapps.connected

    sim_class = SimulationClass(gapps, "run-123.json")

    while not sim_class._finished:
        time.sleep(0.1)

if __name__ == "__main__":
    main()