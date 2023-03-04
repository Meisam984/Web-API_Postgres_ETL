FROM apache/airflow:2.5.1
COPY requirements.txt /requirements.txt
COPY constants.yaml /constants.yaml
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --upgrade pip 
RUN pip install --no-cache-dir -r /requirements.txt
