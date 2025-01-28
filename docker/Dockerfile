FROM asia-docker.pkg.dev/uppsala-devops/application-base-image-repository/py:bullseye-3.13.1

LABEL maintainer="uppsala <developer@sentinelprotocol.io>" \
      description="This image is maintained by the Uppsala team for Sentinel Protocol." \
      version="1.0"

ARG PM2_CONFIG_FILE=pm2_catvconsumer.json
ARG ALLOWED_HOSTS=localhost
ARG CORS_ORIGIN_WHITELIST
ARG CATV_CONSUMER_API_ENV=development
ARG PORTAL_API_VERSION=1.0
ARG CONTAINER_TYPE=catv_consumer_api
ARG EXPOSE_GENERAL_API=false
ARG EXPOSE_FILE_API=false
ARG EXPOSE_SEARCH_API=false
ARG EXPOSE_INTERNAL_API=false
ARG EXPOSE_FREESEARCH_ENDPOINT=false

ENV EXPOSE_GENERAL_API=$EXPOSE_GENERAL_API \
    EXPOSE_FILE_API=$EXPOSE_FILE_API \
    EXPOSE_SEARCH_API=$EXPOSE_SEARCH_API \
    EXPOSE_INTERNAL_API=$EXPOSE_INTERNAL_API \
    EXPOSE_FREESEARCH_ENDPOINT=$EXPOSE_FREESEARCH_ENDPOINT \
    ALLOWED_HOSTS=$ALLOWED_HOSTS \
    CORS_ORIGIN_WHITELIST=$CORS_ORIGIN_WHITELIST \
    CATV_CONSUMER_API_ENV=$CATV_CONSUMER_API_ENV \
    PORTAL_PM2_CONFIG_FILE=$PM2_CONFIG_FILE \
    PORTAL_API_VERSION=$PORTAL_API_VERSION \
    CONTAINER_TYPE=$CONTAINER_TYPE \
    PATH=$PATH:/root/google-cloud-sdk/bin

WORKDIR /app

# Combine package installation steps and clean up
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    apt-utils \
    curl \
    ca-certificates \
    nodejs \
    redis-server \
    gettext && \
    curl --proto '=https' --tlsv1.2 -sSf https://deb.nodesource.com/setup_23.x | bash - && \
    apt-get update && \
    apt-get install -y nodejs && \
    npm install pm2 -g --ignore-scripts && \
    curl --proto '=https' --tlsv1.2 -sSf https://sdk.cloud.google.com | bash && \
    apt-get install --only-upgrade libtiff5 -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./requirements/${CATV_CONSUMER_API_ENV}.txt ./requirements.txt
COPY ./library ./library

# Combine Python package installation steps
RUN pip install --upgrade pip setuptools==75.0.0 && \
    pip install -r /app/requirements.txt && \
    pip install uwsgi==2.0.28 && \
    pip install -e /app/library/indicator-lib/src/py

# Bundle app files
COPY . .
COPY ./deploy ./deploy

# Make translations available
RUN django-admin compilemessages

# Add uppsala user and set permissions
RUN groupadd -g 999 uppsala && \
    useradd -r -u 999 -g uppsala uppsala && \
    chown -R uppsala:uppsala /app && \
    usermod -d /app uppsala

USER uppsala

CMD pm2-runtime /app/deploy/conf/$PORTAL_PM2_CONFIG_FILE