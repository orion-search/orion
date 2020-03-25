# Start with a base image
FROM python:3-onbuild as base
# FROM apache/airflow:master-python3.6-ci
# FROM python:3.6-slim-buster
# FROM continuumio/miniconda3

ARG DB_HOST=localhost
ARG DB_PORT=5432
ARG DB_NAME=postgres
ARG DB_USER=postgres
ARG DB_PASS

ENV orion_prod=postgres+psycopg2://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}
ENV orion_test=postgres+psycopg2://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/postgres

ENV AIRFLOW_HOME /airflow/orion/core
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN ${orion_prod}
ENV AIRFLOW__CORE__EXECUTOR LocalExecutor

FROM base as builder

RUN mkdir /install
WORKDIR /install

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip \
  && pip install --install-option="--prefix=/install" -r /requirements.txt

FROM base

WORKDIR /airflow

COPY --from=builder /install /usr/local
COPY . ./
COPY entrypoint.sh /entrypoint.sh
COPY boto.cfg /etc/boto.cfg


RUN echo $orion_prod \
  && echo $DB_HOST \
  && echo $DB_PORT \
  && echo $DB_NAME \
  && echo $DB_USER \
  && echo $DB_PASS \
  && echo $AIRFLOW_HOME \
  && pip install -e . \
  && airflow initdb

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
CMD ["airflow", "webserver"]