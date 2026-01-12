FROM apache/airflow:3.1.3

USER root
# Install system-level build dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         gcc \
         python3-dev \
         libpq-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Installs your project dependencies
# Note: dbt-postgres will now be able to compile properly
RUN pip install --no-cache-dir \
    paralex \
    pandas \
    psycopg2-binary \
    apache-airflow-providers-postgres \
    dbt-core \
    dbt-postgres
