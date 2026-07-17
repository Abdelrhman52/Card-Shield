# CardShield Flink TaskManager — Flink 1.19.1 + PyFlink

FROM flink:1.19.1-scala_2.12-java11

USER root

# Build Python 3.11 with all optional stdlib modules enabled
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        wget \
        ca-certificates \
        libssl-dev \
        zlib1g-dev \
        libbz2-dev \
        libffi-dev \
        libreadline-dev \
        libsqlite3-dev \
        liblzma-dev \
        xz-utils \
        libncursesw5-dev \
        libgdbm-dev \
        libgdbm-compat-dev \
        libnss3-dev \
        libexpat1-dev \
        tk-dev \
        uuid-dev \
        libdb5.3-dev \
        libmpdec-dev \
        && \
    wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz && \
    tar -xzf Python-3.11.9.tgz && \
    cd Python-3.11.9 && \
    ./configure \
        --enable-optimizations \
        --enable-shared && \
    make -j"$(nproc)" && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && \
    rm -rf Python-3.11.9 Python-3.11.9.tgz && \
    ln -sf /usr/local/bin/python3.11 /usr/local/bin/python && \
    ln -sf /usr/local/bin/python3.11 /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/pip3.11 /usr/local/bin/pip && \
    python --version && \
    python -c "import ssl,bz2,lzma,sqlite3; print('Python modules OK')" && \
    apt-get purge -y build-essential wget && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY fraud_job.py /opt/flink/usrlib/fraud_job.py
COPY transaction.avsc /opt/flink/usrlib/transaction.avsc
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

USER flink