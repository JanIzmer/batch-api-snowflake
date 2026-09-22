# ---------------------------------------------------------------------------
# One image for both roles. Airflow workers and the standalone CLI run exactly
# the same code, so "works on my machine" and "works in the DAG" cannot drift.
# ---------------------------------------------------------------------------
FROM apache/airflow:3.3.2-python3.11

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential git curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/pipeline

# Requirements first: this layer is cached until a dependency actually changes,
# which keeps rebuilds during development down to a few seconds.
COPY --chown=airflow:root requirements.txt /opt/pipeline/requirements.txt
RUN pip install --no-cache-dir -r /opt/pipeline/requirements.txt \
    && pip install --no-cache-dir \
        "dbt-core==1.8.7" \
        "dbt-snowflake==1.8.4" \
        "apache-airflow-providers-snowflake==5.7.0"

COPY --chown=airflow:root pyproject.toml /opt/pipeline/pyproject.toml
COPY --chown=airflow:root src /opt/pipeline/src
COPY --chown=airflow:root contracts /opt/pipeline/contracts
COPY --chown=airflow:root seeds /opt/pipeline/seeds
COPY --chown=airflow:root dbt /opt/pipeline/dbt

RUN pip install --no-cache-dir --no-deps -e /opt/pipeline

ENV PYTHONPATH=/opt/pipeline/src \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/opt/pipeline/dbt

