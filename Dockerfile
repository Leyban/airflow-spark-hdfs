FROM apache/airflow:2.6.3

COPY requirements.txt .
COPY openjdk-8-jdk_8u322-b06-1~deb9u1_amd64.deb .
COPY openjdk-8-jre_8u322-b06-1~deb9u1_amd64.deb .

# RUN pip install -r requirements.txt

USER root

# Java is required in order to spark-submit work
RUN apt-get upgrade &&\
    apt-get update &&\
    apt-get install -y software-properties-common &&\
    add-apt-repository ppa:openjdk-r/ppa &&\
    dpkg -i openjdk-8-jre_8u322-b06-1~deb9u1_amd64.deb &&\
    dpkg -i openjdk-8-jdk_8u322-b06-1~deb9u1_amd64.deb

# Setup JAVA_HOME 
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME