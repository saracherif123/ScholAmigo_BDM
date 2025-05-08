FROM apache/airflow:2.8.1

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3-pip \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install --no-cache-dir \
    boto3 \
    botocore

USER airflow 