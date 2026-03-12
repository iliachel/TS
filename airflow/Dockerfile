FROM apache/airflow:3.1.7

USER root
COPY airflow/requirements.txt /opt/airflow/requirements.txt
RUN chown airflow: /opt/airflow/requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

COPY app /opt/airflow/app
COPY airflow/dags /opt/airflow/dags

ENV PYTHONPATH=/opt/airflow

# Airflow 3 uses api-server; easiest dev entrypoint is standalone
CMD ["bash", "-c", "airflow db migrate && airflow standalone"]
