FROM apache/airflow:2.6.3
USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

COPY ./requirements.txt /
RUN python -m pip install --upgrade pip
RUN pip3 install -r /requirements.txt
COPY --chown=airflow:root ./dags /opt/airflow/dags