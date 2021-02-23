import csv
import datetime
import decimal
import logging
import os
import pathlib
import requests
import signal
import sys
import time
import urllib.parse

log = logging.getLogger('cloudability_api.get_daily_spend')


class Settings:
    def __init__(self):
        self.session = requests.Session()

    @property
    def cloudability_auth_token(self) -> str:
        return os.getenv('CLOUDABILITY_AUTH_TOKEN')

    @property
    def log_format(self) -> str:
        return os.getenv('LOG_FORMAT', '%(levelname)s [%(name)s] %(message)s')

    @property
    def log_level(self) -> str:
        return os.getenv('LOG_LEVEL', 'INFO')

    @property
    def output_file(self) -> pathlib.Path:
        return pathlib.Path(os.getenv('OUTPUT_FILE', '/data/cloudability-daily-spend.csv')).resolve()

    @property
    def report_length_days(self) -> int:
        return int(os.getenv('REPORT_LENGTH_DAYS', '7'))

    @property
    def start_date(self) -> datetime.date:
        env_start_date = os.getenv('START_DATE')
        if env_start_date is None:
            return datetime.date.today() - datetime.timedelta(days=7)
        else:
            return datetime.datetime.strptime(env_start_date, '%Y-%m-%d').date()

    @property
    def vendor_accounts(self) -> list[dict]:
        value = []
        raw = os.getenv('VENDOR_ACCOUNTS').split()
        for r in raw:
            vendor_id, vendor_name = r.split(':', maxsplit=1)
            value.append({'vendor_id': vendor_id, 'vendor_name': vendor_name})
        return value

    @property
    def version(self) -> str:
        return os.getenv('APP_VERSION', 'unknown')


def get_url(settings: Settings, url: str) -> requests.Response:
    response = None
    waiting = True
    while waiting:
        response = settings.session.get(url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            if response.status_code == 502:
                log.warning('502 response, trying again')
                continue
            else:
                raise
        waiting = False
    return response


def clean_currency(value: str) -> decimal.Decimal:
    value = value.lstrip('$')
    value = value.replace(',', '')
    return decimal.Decimal(value)


def parse_result_row(vendor: dict, row: dict) -> dict:
    owner_email = row.get('tag13')
    if owner_email in ('(not set)', '', None):
        owner_email = '(unknown)'
    application_env = row.get('tag8')
    if application_env in ('(not set)', '', None):
        application_env = '(unknown)'
    return {
        'vendor_id': vendor.get('vendor_id'),
        'vendor_name': vendor.get('vendor_name'),
        'resource_id': row.get('resource_identifier'),
        'service_name': row.get('enhanced_service_name'),
        'name': row.get('tag1'),
        'owner_email': owner_email,
        'date': row.get('date'),
        'unblended_cost': clean_currency(row.get('unblended_cost')),
        'adjusted_cost': clean_currency(row.get('adjusted_cost')),
        'usage_hours': decimal.Decimal(row.get('usage_hours')),
        'usage_quantity': decimal.Decimal(row.get('usage_quantity')),
        'application_env': application_env
    }


def submit_job(settings: Settings, url: str) -> int:
    enqueue_response = get_url(settings, url)
    enqueue_data = enqueue_response.json()
    job_id = enqueue_data.get('id')
    log.info(f'Request submitted, job {job_id}')
    return job_id


def wait_for_job(settings: Settings, url: str) -> str:
    job_status = 'requested'
    while job_status not in ('errored', 'finished'):
        time.sleep(5)
        state_response = get_url(settings, url)
        state_data = state_response.json()
        job_status = state_data.get('status')
        log.info(f'Job is {job_status}')
    return job_status


def get_data(settings: Settings):
    base_url = 'https://app.cloudability.com/api/1/reporting/cost'
    token_only = {'auth_token': settings.cloudability_auth_token}
    query = {
        'auth_token': settings.cloudability_auth_token,
        'dimensions': 'resource_identifier,enhanced_service_name,tag1,tag8,tag13,date',
        'metrics': 'unblended_cost,adjusted_cost,usage_hours,usage_quantity'
    }
    total = len(settings.vendor_accounts)
    for i, vendor in enumerate(settings.vendor_accounts, start=1):
        start_date = settings.start_date
        vendor_id = vendor.get('vendor_id')
        query['filters'] = f'vendor_account_identifier=={vendor_id}'
        while start_date < datetime.date.today():
            end_date = start_date + datetime.timedelta(days=settings.report_length_days)
            log.info(f'Requesting data from {start_date} to {end_date} for {vendor_id} ({i} of {total})')
            query['start_date'] = str(start_date)
            query['end_date'] = str(end_date)
            url = f'{base_url}/enqueue?{urllib.parse.urlencode(query)}'
            job_id = submit_job(settings, url)
            url = f'{base_url}/reports/{job_id}/state?{urllib.parse.urlencode(token_only)}'
            job_status = wait_for_job(settings, url)
            if job_status == 'finished':
                log.info(f'Fetching results for job {job_id}')
                url = f'{base_url}/reports/{job_id}/results?{urllib.parse.urlencode(token_only)}'
                results_response = get_url(settings, url)

                for result in results_response.json().get('results'):
                    yield parse_result_row(vendor, result)
            else:
                log.critical(f'Job {job_id} is {job_status}')
            start_date = end_date + datetime.timedelta(days=1)


def main():
    settings = Settings()
    logging.basicConfig(format=settings.log_format, level='DEBUG', stream=sys.stdout)
    log.debug(f'cloudability_api.get_daily_spend {settings.version}')
    if not settings.log_level == 'DEBUG':
        log.debug(f'Setting log level to {settings.log_level}')
    logging.getLogger().setLevel(settings.log_level)

    plural = '' if len(settings.vendor_accounts) == 1 else 's'
    log.info(f'Getting data for {len(settings.vendor_accounts)} vendor account{plural}')

    csv_field_names = [
        'vendor_id', 'vendor_name', 'resource_id', 'service_name', 'name', 'owner_email', 'date', 'unblended_cost',
        'adjusted_cost', 'usage_hours', 'usage_quantity', 'application_env'
    ]
    log.info(f'Writing data to {settings.output_file}')
    with settings.output_file.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_field_names)
        writer.writeheader()
        for row in get_data(settings):
            writer.writerow(row)
    log.info('All done!')


def handle_sigterm(_signal, _frame):
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
