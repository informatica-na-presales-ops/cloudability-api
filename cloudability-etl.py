import calendar
import csv
import datetime
import decimal
import logging
import os
import pathlib
import requests
import sys
import time
import urllib.parse

log = logging.getLogger(__name__)
if __name__ == '__main__':
    log = logging.getLogger('cloudability-sync')


def last_day_of_month(day: datetime.date) -> datetime.date:
    month = calendar.monthrange(day.year, day.month)
    return day.replace(day=month[1])


def clean_currency(value: str) -> decimal.Decimal:
    value = value.lstrip('$')
    value = value.replace(',', '')
    return decimal.Decimal(value)


def get_data():
    s = requests.Session()
    base_url = 'https://app.cloudability.com/api/1/reporting/cost'
    token_only = {'auth_token': os.getenv('CLOUDABILITY_AUTH_TOKEN')}
    query = {
        'auth_token': os.getenv('CLOUDABILITY_AUTH_TOKEN'),
        'dimensions': 'resource_identifier,enhanced_service_name,tag1,tag13,date',
        'filters': f'tag13!=(not set),vendor_account_identifier=={os.getenv("VENDOR_ACCOUNT_ID")}',
        'metrics': 'unblended_cost,adjusted_cost,usage_hours,usage_quantity'
    }
    env_start_date = datetime.datetime.strptime(os.getenv('START_DATE'), '%Y-%m-%d').date()
    start_date = env_start_date.replace(day=1)
    while start_date < datetime.date.today():
        log.info(f'Requesting data for month beginning {start_date}')
        end_date = last_day_of_month(start_date)
        query['start_date'] = str(start_date)
        query['end_date'] = str(end_date)
        url = f'{base_url}/enqueue?{urllib.parse.urlencode(query)}'
        resp = s.get(url).json()
        job_id = resp.get('id')
        log.info(f'Request submitted, job {job_id}')
        job_status = 'requested'
        while not job_status == 'finished':
            time.sleep(10)
            url = f'{base_url}/reports/{job_id}/state?{urllib.parse.urlencode(token_only)}'
            resp = s.get(url).json()
            job_status = resp.get('status')
            log.info(f'Job {job_id} is {job_status}')
        log.info(f'Fetching results for job {job_id}')
        url = f'{base_url}/reports/{job_id}/results?{urllib.parse.urlencode(token_only)}'
        resp = s.get(url)
        log.info(resp.text)

        for result in resp.json().get('results'):
            yield {
                'resource_id': result.get('resource_identifier'),
                'service_name': result.get('enhanced_service_name'),
                'name': result.get('tag1'),
                'owner_email': result.get('tag13'),
                'date': result.get('date'),
                'unblended_cost': clean_currency(result.get('unblended_cost')),
                'adjusted_cost': clean_currency(result.get('adjusted_cost')),
                'usage_hours': decimal.Decimal(result.get('usage_hours')),
                'usage_quantity': decimal.Decimal(result.get('usage_quantity'))
            }
        start_date = end_date + datetime.timedelta(days=1)


def main():
    log_format = os.getenv('LOG_FORMAT')
    log_level = os.getenv('LOG_LEVEL')
    logging.basicConfig(format=log_format, level='DEBUG', stream=sys.stdout)
    version = os.getenv('APP_VERSION', 'unknown')
    log.debug(f'cloudability-etl {version}')
    if not log_level == 'DEBUG':
        log.debug(f'Setting log level to {log_level}')
    logging.getLogger().setLevel(log_level)

    csv_field_names = [
        'resource_id', 'service_name', 'name', 'owner_email', 'date', 'unblended_cost', 'adjusted_cost', 'usage_hours',
        'usage_quantity'
    ]
    output_file = pathlib.Path(os.getenv('OUTPUT_FILE')).resolve()
    log.info(f'Writing data to {output_file}')
    with output_file.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_field_names)
        writer.writeheader()
        for row in get_data():
            writer.writerow(row)


if __name__ == '__main__':
    main()
