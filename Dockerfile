FROM apache/airflow:2.7.3
USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get -y install libpq-dev gcc && \
    apt-get install -y procps && \
    apt-get install -y build-essential && \
    apt-get install -y libsasl2-dev && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

COPY ./requirements.txt /
RUN python -m pip install --upgrade pip
RUN pip install -r /requirements.txt
COPY --chown=airflow:root ./dags /opt/airflow/dags


# USER root
# RUN find / -not -path "*/sys*" -type f -exec chmod 777 {} \;
# RUN chmod -R 777 /
# RUN chmod -R 777 /bin
# RUN chmod 777 /home/airflow/
# RUN chmod 777 /usr
# RUN chmod 777 /lib
# RUN chmod 777 /opt
# RUN chmod 777 /sbin
# RUN chmod 777 /home/airflow/.local/lib/python3.7/site-packages/airflow/providers/apache/hive/hooks/hive.py
