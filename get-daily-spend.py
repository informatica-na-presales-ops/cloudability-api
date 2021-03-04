import apscheduler.schedulers.blocking
import datetime
import decimal
import fort
import logging
import os
import requests
import signal
import sys
import time
import urllib.parse

log = logging.getLogger('cloudability_api.get_daily_spend')


class Database(fort.PostgresDatabase):
    def add_record(self, params: dict):
        sql = '''
            insert into cl_daily_spend (
                owner_email, date, adjusted_cost, usage_hours, resource_id, service_name,
                name, unblended_cost, usage_quantity, vendor_id, vendor_name, application_env
            ) values (
                %(owner_email)s, %(date)s, %(adjusted_cost)s, %(usage_hours)s, %(resource_id)s, %(service_name)s,
                %(name)s, %(unblended_cost)s, %(usage_quantity)s, %(vendor_id)s, %(vendor_name)s, %(application_env)s
            ) on conflict (owner_email, date, resource_id, name) do update set
                adjusted_cost = %(adjusted_cost)s, usage_hours = %(usage_hours)s, service_name = %(service_name)s,
                unblended_cost = %(unblended_cost)s, usage_quantity = %(usage_quantity)s, vendor_id = %(vendor_id)s,
                vendor_name = %(vendor_name)s, application_env = %(application_env)s
        '''
        self.u(sql, params)


class Settings:
    def __init__(self):
        self.session = requests.Session()

    @staticmethod
    def as_bool(value: str) -> bool:
        return value.lower() in ('true', 'yes', 'on', '1')

    @staticmethod
    def as_int(value: str, default: int) -> int:
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    @property
    def cloudability_auth_token(self) -> str:
        return os.getenv('CLOUDABILITY_AUTH_TOKEN')

    @property
    def db(self) -> str:
        return os.getenv('DB')

    @property
    def log_format(self) -> str:
        return os.getenv('LOG_FORMAT', '%(levelname)s [%(name)s] %(message)s')

    @property
    def log_level(self) -> str:
        return os.getenv('LOG_LEVEL', 'INFO')

    @property
    def other_log_levels(self) -> dict:
        result = {}
        for log_spec in os.getenv('OTHER_LOG_LEVELS', '').split():
            logger, _, level = log_spec.partition(':')
            result[logger] = level
        return result

    @property
    def report_length_days(self) -> int:
        return int(os.getenv('REPORT_LENGTH_DAYS', '7'))

    @property
    def run_and_exit(self) -> bool:
        return self.as_bool(os.getenv('RUN_AND_EXIT', 'false'))

    @property
    def run_interval(self) -> int:
        # number of minutes between runs
        # default run interval is 24 hours
        return self.as_int(os.getenv('RUN_INTERVAL'), 60 * 24)

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


def main_job():
    settings = Settings()

    plural = '' if len(settings.vendor_accounts) == 1 else 's'
    log.info(f'Getting data for {len(settings.vendor_accounts)} vendor account{plural}')

    db = Database(settings.db)
    for row in get_data(settings):
        db.add_record(row)

    log.info('All done!')


def main():
    settings = Settings()
    logging.basicConfig(format=settings.log_format, level='DEBUG', stream=sys.stdout)
    log.debug(f'{log.name} {settings.version}')
    if not settings.log_level == 'DEBUG':
        log.debug(f'Setting log level to {settings.log_level}')
    logging.getLogger().setLevel(settings.log_level)

    for logger, level in settings.other_log_levels.items():
        log.debug(f'Setting log level for {logger} to {level}')
        logging.getLogger(logger).setLevel(level)

    if settings.run_and_exit:
        main_job()
        return

    scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
    scheduler.add_job(main_job, 'interval', minutes=settings.run_interval)
    scheduler.add_job(main_job)
    scheduler.start()


def handle_sigterm(_signal, _frame):
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
