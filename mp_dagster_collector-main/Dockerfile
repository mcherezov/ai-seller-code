FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/mp_data_collector \
    DAGSTER_HOME=/opt/mp_data_collector/dagster_home

WORKDIR /opt/mp_data_collector

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT bash -c "\
  rm -rf /opt/mp_data_collector/dagster_home/instance_heartbeats && \
  python init_db_autogen.py && \
  dagster dev --workspace workspace.yaml --host 0.0.0.0 --port 3000"
