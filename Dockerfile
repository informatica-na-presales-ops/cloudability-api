FROM python:3.8.2-alpine3.11

COPY requirements.txt /cloudability-etl/requirements.txt

RUN /usr/local/bin/pip install --no-cache-dir --requirement /cloudability-etl/requirements.txt

COPY cloudability-etl.py /cloudability-etl/cloudability-etl.py

ENV APP_VERSION="2020.6" \
    PYTHONUNBUFFERED="1"

ENTRYPOINT ["/usr/local/bin/python"]
CMD ["/cloudability-etl/cloudability-etl.py"]

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.version="${APP_VERSION}"
