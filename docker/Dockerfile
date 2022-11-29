FROM 821988754834.dkr.ecr.ap-southeast-1.amazonaws.com/oss/python:3.10.5-buster

MAINTAINER uppsala <developer@sentinelprotocol.io>

ARG PM2_CONFIG_FILE=pm2_catvconsumer.json
ARG EXPOSE_GENERAL_API=false
ARG EXPOSE_FILE_API=false
ARG EXPOSE_SEARCH_API=false
ARG EXPOSE_INTERNAL_API=false
ARG EXPOSE_FREESEARCH_ENDPOINT=false
ARG SLACK_URL=https://hooks.slack.com/services/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
ARG ALLOWED_HOSTS=localhost
ARG CORS_ORIGIN_WHITELIST
ARG CATV_CONSUMER_API_ENV=development
ARG PORTAL_API_VERSION=1.0
ARG CONTAINER_TYPE=catv_consumer_api

ENV EXPOSE_GENERAL_API=$EXPOSE_GENERAL_API
ENV EXPOSE_FILE_API=$EXPOSE_FILE_API
ENV EXPOSE_SEARCH_API=$EXPOSE_SEARCH_API
ENV EXPOSE_INTERNAL_API=$EXPOSE_INTERNAL_API
ENV EXPOSE_FREESEARCH_ENDPOINT=$EXPOSE_FREESEARCH_ENDPOINT
ENV ALLOWED_HOSTS=$ALLOWED_HOSTS
ENV CORS_ORIGIN_WHITELIST=$CORS_ORIGIN_WHITELIST
ENV CATV_CONSUMER_API_ENV=$CATV_CONSUMER_API_ENV
ENV PORTAL_PM2_CONFIG_FILE=$PM2_CONFIG_FILE
ENV PORTAL_API_VERSION=$PORTAL_API_VERSION
ENV CONTAINER_TYPE=$CONTAINER_TYPE

WORKDIR /app

# Install packages
# Install nodejs and pm2
RUN curl -sL https://deb.nodesource.com/setup_11.x | bash -
RUN apt-get update && \
    apt-get install -y \
    nodejs \
    redis-server \
    gettext && \
    npm install pm2@3.5.1 -g

# Install requirements
COPY ./requirements/${CATV_CONSUMER_API_ENV}.txt ./requirements.txt
COPY ./library ./library
RUN pip install --upgrade pip setuptools==58.0.0 && \
    pip install -r /app/requirements.txt && \
    pip install uwsgi && \
    pip install -e /app/library/indicator-lib/src/py

# Bundle APP files
COPY . .
COPY ./deploy ./deploy

# Make translations available
RUN django-admin compilemessages

# Add uppsala user.

RUN groupadd -g 999 uppsala && \
    useradd -r -u 999 -g uppsala uppsala && \
    chown uppsala:uppsala /app && \
    usermod -d /app uppsala
USER uppsala

# RUN
CMD pm2-runtime start /app/deploy/conf/$PORTAL_PM2_CONFIG_FILE --web
