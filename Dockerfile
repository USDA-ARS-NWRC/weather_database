# WeatherDatabase is build on the python image
FROM python:3.5

MAINTAINER Scott Havens <scott.havens@ars.usda.gov>

RUN mkdir /code
COPY . / /code/WeatherDatabase/

RUN cd /code/WeatherDatabase \
    && python3 -m pip install --upgrade pip \
    && python3 -m pip install setuptools wheel \
    && python3 -m pip install -r /code/WeatherDatabase/requirements_dev.txt \
    && python3 setup.py install \
    && rm -r /root/.cache/pip

WORKDIR /code/WeatherDatabase/
CMD ["python"]









