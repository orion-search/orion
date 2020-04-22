# Start with a base image
FROM python:3-onbuild as base

ARG DB_HOST=localhost
ARG DB_PORT=5432
ARG DB_NAME=postgres
ARG DB_USER=postgres
ARG DB_PASS=""

ENV orion_prod=postgres+psycopg2://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}

# Used for unit tests
ENV orion_test=postgres+psycopg2://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/postgres

# Stores Airflow task run metadata
ENV airflow_db=postgres+psycopg2://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/airflow

ENV AIRFLOW_HOME /airflow/orion/core
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN ${airflow_db}
ENV AIRFLOW__CORE__EXECUTOR LocalExecutor
ENV AIRFLOW__CORE__DAGS_FOLDER /airflow/orion/core/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW_PORT=9000

FROM base as builder

RUN mkdir /install
WORKDIR /install

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip \
  && pip install --install-option=“--prefix=/install” -r /requirements.txt

FROM base

WORKDIR /airflow

COPY --from=builder /install /usr/local
COPY . ./
COPY entrypoint /entrypoint
COPY boto.cfg /etc/boto.cfg


RUN pip install -e .

EXPOSE 8080

ENTRYPOINT ["/entrypoint"]
