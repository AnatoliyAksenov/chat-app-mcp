FROM python:3.12-slim

ARG HTTP_PROXY
ARG HTTPS_PROXY

ARG GITLAB_URL
ARG GITLAB_TOKEN
ARG PROJECT_PATH
ARG AIRFLOW_URL
ARG AIRFLOW_USER
ARG AIRFLOW_PASSWORD

ENV GITLAB_URL=${GITLAB_URL}
ENV GITLAB_TOKEN=${GITLAB_TOKEN}
ENV PROJECT_PATH=${PROJECT_PATH}
ENV AIRFLOW_URL=${AIRFLOW_URL}
ENV AIRFLOW_USER=${AIRFLOW_USER}
ENV AIRFLOW_PASSWORD=${AIRFLOW_PASSWORD}
ENV USE_PROXY=${HTTPS_PROXY}

RUN mkdir /app

WORKDIR /app

COPY . .

RUN echo GITLAB_URL=$GITLAB_URL > .env && \
    echo GITLAB_TOKEN=$GITLAB_TOKEN >> .env && \
    echo PROJECT_PATH=$PROJECT_PATH >> .env && \
    echo AIRFLOW_URL=$AIRFLOW_URL >> .env && \
    echo AIRFLOW_USER=$AIRFLOW_USER >> .env && \
    echo AIRFLOW_PASSWORD=$AIRFLOW_PASSWORD >> .env 

RUN if [ -n "$HTTP_PROXY" ]; then \
      pip install --proxy "$HTTP_PROXY" poetry==2.1.3; \
    else \
      pip install poetry==2.1.3; \
    fi && \
    poetry install --no-root

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--forwarded-allow-ips", "*"]