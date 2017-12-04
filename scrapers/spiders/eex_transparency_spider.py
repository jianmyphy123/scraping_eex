# -*- coding: utf-8 -*-

import datetime
import time
import json

import scrapy

from selenium.common import exceptions as selenium_exceptions
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from bs4 import BeautifulSoup

class EexTransparencySpider(scrapy.Spider):
    name = 'eex_availability'
    history_url_list = ['https://www.eex-transparency.com/homepage/power/austria/production/availability/non-usability-/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/belgium/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/switzerland/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/czech-republic/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/germany/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/great-britain/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/hungary/production/availability/non-usability/non-usability-history',
                        'https://www.eex-transparency.com/homepage/power/italy/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/the-netherlands/production/availability/non-usability/non-usability-history-'
                        ]

    current_url_list = ['https://www.eex-transparency.com/homepage/power/austria/storage/availability/non-usability',
                        'https://www.eex-transparency.com/homepage/power/belgium/production/availability/non-usability',
                        'https://www.eex-transparency.com/homepage/power/switzerland/production/availability/non-usability',
                        'https://www.eex-transparency.com/homepage/power/germany/production/availability/non-usability',
                        'https://www.eex-transparency.com/homepage/power/great-britain/production/availability/non-usability',
                        'https://www.eex-transparency.com/homepage/power/hungary/production/availability/non-usability/non-usability-current',
                        'https://www.eex-transparency.com/homepage/power/italy/production/availability/non-usability',
                        'https://www.eex-transparency.com/homepage/power/the-netherlands/production/availability/non-usability'
                        ]


    custom_settings = {
        'CONCURRENT_REQUESTS': 1, # Because of the browser automation
        'ITEM_PIPELINES': {
            'scrapers.pipelines.CSVPipeline': 400,
            'scrapers.pipelines.PostgrePipeline': 500
        }
    }

    def __init__(self,
                 scrape_dir='csv',
                 scrape_log='scrape.log',
                 start=datetime.datetime.utcnow().strftime("%Y-%m-%d"),
                 end=datetime.datetime.utcnow().strftime("%Y-%m-%d"),
                 table='',
                 mode='recent'):
        super().__init__()
        self.scrape_dir = scrape_dir

        self.scrape_log = scrape_log
        self.real_date  = datetime.datetime.strptime(start, '%Y-%m-%d')
        self.cur_date   = datetime.datetime.strptime(start, '%Y-%m-%d')
        self.start      = datetime.datetime.strptime(start, '%Y-%m-%d')
        self.end        = datetime.datetime.strptime(end, '%Y-%m-%d')
        if table:
            self.table = table
        else:
            self.table = self.name

        self.mode = mode
        self.scraper = ScrapeJS()
        self.retrycount = 0

        self.driver = webdriver.PhantomJS('./phantomjs/mac/phantomjs')

        self.failed_urls = []
        self.countries = ['austria', 'belgium', 'switzerland', 'czech-republic', 'germany',
                          'great-britain', 'hungary', 'italy', 'the-netherlands']

        if scrape_log:
            try:
                with open(scrape_log, mode='r') as log_file:
                    self.scrape_info = json.load(log_file)
                    if 'scraping_times' not in self.scrape_info.keys():
                        self.scrape_info['scraping_times'] = []
            except OSError as e:
                if e.errno == 2:
                    print("Log file does not exist and will be created.")
                    self.scrape_info = {
                        'loaded_dates': [],
                        'skipped_dates': [],
                        'scraping_times': []
                    }
                else:
                    raise e
        else:
            self.scrape_info = {
                'loaded_dates': [],
                'skipped_dates': [],
                'scraping_times': []
            }


        self.scrape_info['scraping_times'].append(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        with open(self.scrape_log, mode='w+') as log_file:
            json.dump(self.scrape_info, log_file, indent=4)

    def start_requests(self):
        yield scrapy.Request('https://www.eex-transparency.com/', callback=self.start_requests_selenium)

    def start_requests_selenium(self, response):
        print("Connection OK. Start scraping...")
        if self.mode == 'recent':
            for url in self.current_url_list:
                print("Current for country: ", url)
                items = self.parse_current(url)
                if items is None:
                    yield
                else:
                    for item in items:
                        yield item
                    time.sleep(2)
        else:
            for url in self.history_url_list:
                print("History for country: ", url)
                items = self.parse(url)
                if items is None:
                    yield
                else:
                    for item in items:
                        yield item
                    time.sleep(10)

    def parse(self, url):
        self.driver.get(url)
        self.real_date = self.start
        self.cur_date = self.start

        while self.real_date <= self.end and self.cur_date <= self.end :
            print('[*] Loading page')
            page_loaded = self._load_date(self.cur_date)

            if page_loaded:
                pass
            else:
                print("Unable to load page. Skipping.")
                with open(self.scrape_log, mode='w+') as log_file:
                    json.dump(self.scrape_info, log_file, indent=4)
                self.cur_date = self.cur_date + datetime.timedelta(days=1)
                continue

            try:
                page_ok = self._verify_page()
            except Exception as e:
                page_ok=False

            if page_ok:
                pass
            else:
                if self.retrycount > 2:
                    print("Unable to load page. Skipping.")
                    with open(self.scrape_log, mode='w+') as log_file:
                        json.dump(self.scrape_info, log_file, indent=4)
                    self.cur_date = self.cur_date + datetime.timedelta(days=1)
                    self.retrycount = 0
                    continue
                else:
                    self.retrycount += 1
                    continue

            print("[*] Parsing page")
            data_object = self.driver.execute_script(self.scraper.get_table_data())

            items = self.parse_data_object(data_object, self.real_date)
            if items is None:
                print('Items not found in that url: ', url)
                yield
            else:
                for item in items:
                    yield item

                self.scrape_info['loaded_dates'].append(self.real_date.strftime('%Y-%m-%d'))
                with open(self.scrape_log, mode='w+') as log_file:
                    json.dump(self.scrape_info, log_file, indent=4)
                self.cur_date = self.cur_date + datetime.timedelta(days=1)

    def parse_data_object(self, data_object, parse_date):
        """
        Parses data object to items/
        :param data_object: Source data object.
        :param parse_date: Parse date for the pipeline.
        :return: Yields availability item.
        """
        if data_object is None:
            yield
        else:
            for record in data_object:
                item = {
                    'parse_date': parse_date,
                    'type': record['type'],
                    'company': record['short_name'],
                    'facility': record['prodcon'],
                    'unit': record['unit'],
                    'fuel': record['fuel'] if 'fuel' in record.keys() else "" ,
                    'control_area': record['connecting_area'],
                    'begin_ts': datetime.datetime.fromtimestamp(record['begin'] / 1000).strftime("%Y-%m-%dT%H:%M:%S"),
                    'end_ts': datetime.datetime.fromtimestamp(record['end'] / 1000).strftime("%Y-%m-%dT%H:%M:%S"),
                    'limitation': record['energy_limitation'],
                    'reason': record['reason'],
                    'status': record['canceled'],
                    'event_id': record['event_id'],
                    'last_update': datetime.datetime.fromtimestamp(record['modify_timestamp'] / 1000).strftime("%Y-%m-%dT%H:%M:%S")
                }

                yield item

    def _verify_page(self):
        """
        Verifies that a page contains up-to-date data
        :return:
        """

        ts_xpath = '//div[@class="timestamp"]'
        table_dates = self.driver.find_element_by_xpath(ts_xpath).text
        print("Page loaded: ", table_dates)

        data_from = table_dates.split()[2]
        data_to = table_dates.split()[4]

        if data_from != data_to:
            return False

        self.real_date = datetime.datetime.strptime(data_to, '%Y/%m/%d')

        if self.real_date < self.start:
            print('Date too early:', self.real_date)
            raise ValueError('Date too early:', self.real_date)

        if self.real_date > self.end:
            print('Date too late:', self.real_date)
            raise ValueError('Date too early:', self.real_date)

        return True

    def _load_date(self, date):
        """
        Loads particular date.

        :param date: Date to load
        :return: Returns True on success.
        """

        print("---- Loading date: ", date, '----')

        try:
            self.driver.execute_script(self.scraper.set_dates(date, date))
            self.driver.execute_script(self.scraper.load_data())
        except selenium_exceptions.WebDriverException as e:
            print("LOAD DATE ERROR:", e.msg)
            print("Dates load error. Retrying")
            return False

        time.sleep(1)

        ts_xpath = '//div[@class="timestamp"]'
        try:
            WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, ts_xpath)))
            return True
        except TimeoutException:
            print("ERROR: Page load timeout or no data reported.")
            return False

    def parse_current(self, url):
        self.driver.get(url)

        time.sleep(1)

        ts_xpath = '//div[@class="timestamp"]'
        try:
            WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, ts_xpath)))
        except TimeoutException:
            print("ERROR: Page load timeout or no data reported.")

        print("[*] Parsing page")
        data_object = self.driver.execute_script(self.scraper.getCurrentTableData())
        items = self.parse_data_object(data_object, self.real_date)

        if items is None:
            print('Items not found in that url: ', url)
            yield
        else:
            for item in items:
                yield item

            self.scrape_info['loaded_dates'].append(self.real_date.strftime('%Y-%m-%d'))
            with open(self.scrape_log, mode='w+') as log_file:
                json.dump(self.scrape_info, log_file, indent=4)


class ScrapeJS(object):
    """A helper class that generates javascript snippets to use with browser."""
    def __init__(self):
        self._definitions = {
            'getTableData':
                ('function getTableData() {\n'
                    'var e = document.getElementById("from");\n'
                    'var sc = angular.element(e).scope();\n'
                    'var rows_ng = sc.eventData;\n'
                    'return rows_ng;\n'
                '}\n'
                ),
            'getCurrentTableData':
                ('function getCurrentTableData() {\n'
                    'var e = document.getElementsByClassName("timestamp");\n'
                    'var sc = angular.element(e).scope();\n'
                    'var rows_ng = sc.data;\n'
                    'return rows_ng;\n'
                '}\n'
                ),
            'setDates':
                ('function setDates(fromDate, tDate) {\n'
                    'var e = document.getElementById("from");\n'
                    'var sc = angular.element(e).scope();\n'
                    'var from = moment(fromDate);\n'
                    'var to = moment(tDate);\n'
                    'sc.to = to.toDate();\n'
                    'sc.$apply();\n'
                    '$("#from").blur();\n'
                    'sc.from = from.toDate();\n'
                    'sc.$apply();\n'
                    '$("#from").blur();\n'
                 '}\n'
                ),
            'loadData':
                ('function loadData() {\n'
                    '$("#from").blur();\n'
                 '}\n'
                 )
        }

    def definitions(self):
        """Returns a string with function definitions."""
        return '\n'.join(self._definitions.values())

    def get_table_data(self):
        """Returns JavaScript to get table data."""
        return self._definitions['getTableData'] + '\n' + 'return getTableData();'

    def getCurrentTableData(self):
        """Returns JavaScript to get current table data."""
        return self._definitions['getCurrentTableData'] + '\n' + 'return getCurrentTableData();'

    def load_data(self):
        """Returns Javascript that reloads data on the page."""
        return self._definitions['loadData'] + '\n' + 'loadData();'

    def set_dates(self, fromDate, toDate):
        """Returns JavaScript to set dates interval to load."""
        return self._definitions['setDates'] + '\n'\
               + 'setDates("{0}", "{1}");'.format(fromDate.strftime("%Y-%m-%d"), toDate.strftime("%Y-%m-%d"))
