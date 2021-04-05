FROM python:3.9.3-alpine3.13

RUN /sbin/apk add --no-cache libpq

COPY requirements.txt /cloudability-api/requirements.txt
RUN /usr/local/bin/pip install --no-cache-dir --requirement /cloudability-api/requirements.txt

COPY get-daily-spend.py /cloudability-api/get-daily-spend.py

ENV APP_VERSION="2021.1" \
    PYTHONUNBUFFERED="1"

ENTRYPOINT ["/usr/local/bin/python"]
CMD ["/cloudability-api/get-daily-spend.py"]

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.version="${APP_VERSION}"
