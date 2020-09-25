FROM python:3.8.6-alpine3.12

COPY requirements.txt /cloudability-api/requirements.txt

RUN /usr/local/bin/pip install --no-cache-dir --requirement /cloudability-api/requirements.txt

COPY write-data-to-csv.py /cloudability-api/write-data-to-csv.py

ENV APP_VERSION="2020.10" \
    PYTHONUNBUFFERED="1"

ENTRYPOINT ["/usr/local/bin/python"]
CMD ["/cloudability-api/write-data-to-csv.py"]

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.version="${APP_VERSION}"
