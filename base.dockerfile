ARG PYTHON_VERSION
FROM artifactory.statnett.no/registry/statnett/python-slim-$PYTHON_VERSION:latest
RUN apt-get update && apt-get install -y librdf0
