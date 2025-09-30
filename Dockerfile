# Use the base application container to allow the application to be controlled
# from the gridappsd container.
FROM gridappsd/application-base-python:main

# Add the TIMESTAMP variable to capture the build information from 
# the travis docker build command and add them to the image.
ARG TIMESTAMP
RUN echo $TIMESTAMP > /dockerbuildversion.txt 

# Pick a spot to put our application code
# (note gridappsd-python is located at /usr/src/gridappsd-python)
# and is already installed in the app-container-base environment.
WORKDIR /usr/src/gridappsd-sample

# Add dependencies to the requirements.txt file before
# uncommenting the next two lines
# COPY requirements.txt ./
# RUN RUN pip install --no-cache-dir -r requirements.txt

# Copy all of the source over to the container.
COPY . .

# Use a symbolic link to the sample app rather than having to
# mount it at run time (note can still be overriden in docker-compose file)
RUN ln -s /usr/src/gridappsd-sample/sample_app.config /appconfig
