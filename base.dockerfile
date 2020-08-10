ARG PYTHON_VERSION
FROM artifactory.statnett.no/registry/curlimages/curl as builder
RUN for cert in StatnettRootCA2.crt StatnettRootCA4.crt; do \
    curl -o /tmp/$cert \
    https://artifactory.statnett.no/artifactory/datascience-generic-local/cert.tar.gz/cert/$cert; done;

FROM artifactory.statnett.no/registry/python:$PYTHON_VERSION-slim

COPY --from=builder /tmp/StatnettRootCA*.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

ARG PIP_INDEX_URL
ENV PIP_INDEX_URL="$PIP_INDEX_URL"

RUN python -m pip install --upgrade --index-url https://artifactory.statnett.no/api/pypi/pypi_statnett_ia_virtual/simple pip \
    && pip config set global.index https://artifactory.statnett.no/api/pypi/pypi_statnett_ia_virtual/simple \
    && pip config set global.index-url https://artifactory.statnett.no/api/pypi/pypi_statnett_ia_virtual/simple

RUN  sed -i 's http://deb.debian.org http://x1-a-utvtartfp1.statnett.no:8081/artifactory/debian_remote/ g' /etc/apt/sources.list \
     && sed -i 's http://security.debian.org http://x1-a-utvtartfp1.statnett.no:8081/artifactory/debian_security_remote/ g' /etc/apt/sources.list \
     && apt-get clean && apt-get update

RUN apt-get update && apt-get install -y librdf0
