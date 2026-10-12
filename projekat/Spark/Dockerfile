FROM apache/spark:3.5.3

USER root

ARG PYTHON_VERSION=3.11.13

# Install dependencies required to compile Python.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        wget \
        zlib1g-dev \
        libncursesw5-dev \
        libgdbm-dev \
        libnss3-dev \
        libssl-dev \
        libreadline-dev \
        libffi-dev \
        libsqlite3-dev \
        libbz2-dev \
        liblzma-dev \
        uuid-dev \
        xz-utils && \
    rm -rf /var/lib/apt/lists/*

# Download, compile and install Python 3.11.
RUN wget -q \
        "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz" \
        -O /tmp/Python.tgz && \
    tar -xzf /tmp/Python.tgz -C /tmp && \
    cd "/tmp/Python-${PYTHON_VERSION}" && \
    ./configure --prefix=/usr/local --with-ensurepip=install && \
    make -j2 && \
    make altinstall && \
    cd / && \
    rm -rf /tmp/Python.tgz "/tmp/Python-${PYTHON_VERSION}" && \
    /usr/local/bin/python3.11 --version

ENV PYSPARK_PYTHON=/usr/local/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11

ARG POSTGRES_JDBC_VERSION=42.7.8
RUN mkdir -p /opt/spark/jars && \
    wget -q "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        -O "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" && \
    chown 185:0 "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar"

USER 185