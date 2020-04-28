FROM python:3.8.2-alpine3.11

COPY requirements.txt /cloudability-etl/requirements.txt

RUN /usr/local/bin/pip install --no-cache-dir --requirement /cloudability-etl/requirements.txt

COPY cloudability-etl.py /cloudability-etl/cloudability-etl.py

ENV APP_VERSION-="0.1.0" \
    CLOUDABILITY_AUTH_TOKEN="" \
    LOG_FORMAT="%(levelname)s [%(name)s] %(message)s" \
    LOG_LEVEL="INFO" \
    OUTPUT_FILE="/data/cloudability-daily-spend.csv" \
    PYTHONUNBUFFERED="1" \
    RETRY_COUNT="30" \
    RETRY_INTERVAL="10" \
    START_DATE="2019-01-01" \
    VENDOR_ACCOUNT_ID=""

ENTRYPOINT ["/usr/local/bin/python"]
CMD ["/cloudability-etl/cloudability-etl.py"]

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.version="${APP_VERSION}"
