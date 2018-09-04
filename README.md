# GridAPPS-D Sample Application

## Purpose

The purpose of this repository is to document the chosen way of registering and running applications within a 
GridAPPS-D deployment.

## Sample Application Layout

The following is the recommended structure for applications working with gridappsd:

    ```` bash
    .
    ├── README.md
    └── sample_app
        ├── requirements.txt
        ├── sample_app
        │   └── runsample.py
        └── sample_app.config
    
    ````

## Requirements

1. Docker ce version 17.12 or better.  You can install this via the docker_install_ubuntu.sh script.  (note for mint you will need to modify the file to work with xenial rather than ubuntu generically)

2. Please clone the repository <https://github.com/GRIDAPPSD/gridappsd-docker> (refered to as gridappsd-docker 
   repository) next to this repository (they should both have the same parent folder)

    ```` bash
    .
    ├── gridappsd-docker
    └── gridappsd-sample-app
    ````

## Creating the sample-app application container

1.  From the command line execute the following commands to build the sample-app container

    ```` console
    osboxes@osboxes> cd gridappsd-sample-app
    osboxes@osboxes> docker build --network=host -t sample-app .
    ````

1.  Add the following to the gridappsd-docker/docker-compose.yml file

    ```` yaml
    sampleapp:
      image: sample-app
      depends_on: 
        gridappsd    
    ````

1.  Run the docker application 

    ```` console
    osboxes@osboxes> cd gridappsd-docker
    osboxes@osboxes> ./run.sh
    
    # you will now be inside the container, the following starts gridappsd
    
    gridappsd@f4ede7dacb7d:/gridappsd$ ./run-gridappsd.sh
    
    ````

