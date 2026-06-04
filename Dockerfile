
FROM python:3.11-slim-bookworm
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    procps \
    openjdk-17-jre \
    findutils \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME \
    && rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    spark-nlp==5.5.1 \
    pandas \
    pymongo \
    streamlit \
    plotly \
    kafka-python \
    scikit-learn \
    numpy
WORKDIR /app
COPY spark_nlp_complete.py /app/
COPY live_dashboard.py /app/
COPY *.py /app/
USER root
CMD ["/bin/bash"]