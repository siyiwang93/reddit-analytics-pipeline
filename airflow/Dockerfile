FROM apache/airflow:2.9.0

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
