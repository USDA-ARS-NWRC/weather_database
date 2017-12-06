# Weather Database

[![GitHub version](https://badge.fury.io/gh/USDA-ARS-NWRC%2FWeatherDatabase.svg)](https://badge.fury.io/gh/USDA-ARS-NWRC%2FWeatherDatabase)

The Weather Database project creates a MySQL database to ingest and manage weather station data from multiple sources. Included in
the repository are the schema model and everything needed to generate a weather station database. The `wxdb` package is able
to interact with the database and grab metadata and station data from multiple sources. Currently, the sources supported are:

* [California Data Exchange Center (CDEC)](https://cdec.water.ca.gov/)
* [Mesowest API](https://synopticlabs.org/api/) 

Through the configuration file, `wxdb`can be setup to automatically download all desired stations for a given client and source.


## Installation


It is preferable to use a Python [virtual environment](https://virtualenv.pypa.io) to reduce the possibility of a dependency issue.

1. Create a virtualenv and activate it.

    ````
    virtualenv wxdbenv
    source wxdbenv/bin/activate
    ```

**Tip:** The developers recommend using an alias to quickly turn on
and off your virtual environment.


2. Clone WeatherDatabase source code from the USDA-ARS-NWRC github.

    ```
    git clone https://github.com/USDA-ARS-NWRC/WeatherDatabase.git
    ```
    
3. Change directories into the `WeatherDatabase` directory. Install the python requirements.
   After the requirements are done, install `WeatherDatabase`.

    ```
    cd WeatherDatabase
    pip install -r requirements.txt
    python setup.py install
    ```

To install in developer mode `pip install -e .`

## Docker


To make management easier, the authors recommend using [Docker](https://docker.com) which aides in the management, deployment,
and automation of downloads to the database. The official Docker image is found at the 
[docker_weather_database](https://github.com/USDA-ARS-NWRC/docker_weather_database).

