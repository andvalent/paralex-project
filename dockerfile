FROM apache/airflow:3.1.3

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends vim \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Installs paralex (DAG 1) and pandas/postgres libraries (DAG 2)
RUN pip install --no-cache-dir paralex pandas psycopg2-binary apache-airflow-providers-postgres