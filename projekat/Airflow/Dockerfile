FROM apache/airflow:3.1.5-python3.10

COPY requirements.txt .

USER root

RUN mkdir /shared && chown -R airflow /shared

RUN apt-get update

RUN echo "deb http://deb.debian.org/debian unstable main contrib non-free" >> /etc/apt/sources.list

RUN echo "Package: * \
Pin: release a=stable \
Pin-Priority: 900 \
\
Package: * \
Pin: release a=unstable\
Pin-Priority: 50" > /etc/apt/preferences.d/99pin-unstable

RUN apt-get update && apt-get install -y \
    gcc \
    libkrb5-dev \
    openjdk-11-jdk \
    curl \
 && apt-get clean

# Install Spark
ENV SPARK_VERSION=3.0.1
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/opt/spark

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt \
 && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/

RUN mkdir -p "${SPARK_EXTRA_JARS_DIR}"

# ---- Install curl (if not present) ----
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# ---- Download MongoDB Spark Connector ----

# Mongo Spark Connector
RUN curl -fL \
  https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.0/mongo-spark-connector_2.12-10.2.0.jar \
  -o /opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar

# MongoDB Java Driver dependencies
RUN curl -fL \
  https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.8.2/mongodb-driver-sync-4.8.2.jar \
  -o /opt/spark/jars/mongodb-driver-sync-4.8.2.jar

RUN curl -fL \
  https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.8.2/mongodb-driver-core-4.8.2.jar \
  -o /opt/spark/jars/mongodb-driver-core-4.8.2.jar

RUN curl -fL \
  https://repo1.maven.org/maven2/org/mongodb/bson/4.8.2/bson-4.8.2.jar \
  -o /opt/spark/jars/bson-4.8.2.jar
# ---- Fix permissions ----
RUN chown root:root /opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar
RUN chown root:root /opt/spark/jars/mongodb-driver-sync-4.8.2.jar
RUN chown root:root /opt/spark/jars/mongodb-driver-core-4.8.2.jar
RUN chown root:root /opt/spark/jars/bson-4.8.2.jar

# ---- Spark Kafka Connector ----
RUN curl -fL \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.1/spark-sql-kafka-0-10_2.12-3.0.1.jar \
  -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.0.1.jar

RUN curl -fL \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.0.1/spark-token-provider-kafka-0-10_2.12-3.0.1.jar \
  -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.0.1.jar

RUN curl -fL \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar \
  -o /opt/spark/jars/kafka-clients-2.4.1.jar

RUN chown root:root /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.0.1.jar \
    && chown root:root /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.0.1.jar \
    && chown root:root /opt/spark/jars/kafka-clients-2.4.1.jar

RUN curl -fL \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.6.2/commons-pool2-2.6.2.jar \
  -o /opt/spark/jars/commons-pool2-2.6.2.jar

# Environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow 

RUN pip install -r requirements.txt

USER airflow

WORKDIR /opt/airflow