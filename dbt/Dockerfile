FROM python:3.13-slim

WORKDIR /dbt

RUN pip install --no-cache-dir dbt-clickhouse==1.9.8

COPY dbt /dbt

ENV DBT_PROFILES_DIR=/dbt

ENTRYPOINT ["dbt"]
CMD ["--version"]
