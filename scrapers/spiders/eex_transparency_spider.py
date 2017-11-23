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
    # history_url_list = ['https://www.eex-transparency.com/homepage/power/austria/production/availability/non-usability-/non-usability-history-',
    #                     'https://www.eex-transparency.com/homepage/power/belgium/production/availability/non-usability/non-usability-history-',
    #                     'https://www.eex-transparency.com/homepage/power/switzerland/production/availability/non-usability/non-usability-history-',
    #                     'https://www.eex-transparency.com/homepage/power/czech-republic/production/availability/non-usability/non-usability-history-',
    #                     'https://www.eex-transparency.com/homepage/power/germany/production/availability/non-usability/non-usability-history-',
    #                     'https://www.eex-transparency.com/homepage/power/great-britain/production/availability/non-usability/non-usability-history-',
    #                     'https://www.eex-transparency.com/homepage/power/hungary/production/availability/non-usability/non-usability-history',
    #                     'https://www.eex-transparency.com/homepage/power/italy/production/availability/non-usability/non-usability-history-',
    #                     'https://www.eex-transparency.com/homepage/power/the-netherlands/production/availability/non-usability/non-usability-history-'
    #                     ]
    history_url_list = ['https://www.eex-transparency.com/homepage/power/austria/production/availability/non-usability-/non-usability-history-',
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
                 scrape_dir='',
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

        self.driver = webdriver.PhantomJS('./phantomjs')

        self.failed_urls = []
        self.countries = ['austria', 'belgium', 'switzerland', 'czech-republic', 'germany',
                          'great-britain', 'hungary', 'italy', 'the-netherlands']

        if scrape_log:
            try:
                with open(scrape_log, mode='r') as log_file:
                    self.scrape_info = json.load(log_file)
            except OSError as e:
                if e.errno == 2:
                    print("Log file does not exist and will be created.")
                    self.scrape_info = {
                        'loaded_dates': [],
                        'skipped_dates': []
                    }
                else:
                    raise e
        else:
            self.scrape_info = {
                'loaded_dates': [],
                'skipped_dates': []
            }

    def start_requests(self):
        yield scrapy.Request('https://www.eex-transparency.com/', callback=self.start_requests_selenium)

    def start_requests_selenium(self, response):
        print("Connection OK. Start scraping...")
        if self.mode == 'recent':
            for url in self.current_url_list:
                print("Current for country: ", url)
                for item in self.parse_current(url):
                    yield item
                time.sleep(2)
        else:
            for url in self.history_url_list:
                print("History for country: ", url)
                for item in self.parse(url):
                    yield item
                time.sleep(10)

    def parse(self, url):
        self.driver.get(url)
        self.real_date = self.start
        self.cur_date = self.start

        # ts_xpath = '//input[@id="to"]'
        # try:
        #     WebDriverWait(self.driver, 7).until(EC.presence_of_element_located((By.XPATH, ts_xpath)))
        # except TimeoutException:
        #     print("ERROR: Page load timeout or no data reported.")
        #     print(self.driver.page_source)
        #     return None

        # while self.real_date < self.end and self.cur_date < (self.end + datetime.timedelta(days=5)) :
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
            for item in self.parse_data_object(data_object, self.real_date):
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
        for record in data_object:
            item = {
                'parse_date': parse_date,
                'type': record['type'],
                'unitname': record['unit'],
                'area': None,
                'fuel_type': record['fuel'],
                'begin': datetime.datetime.fromtimestamp(record['begin'] / 1000).strftime("%Y-%m-%dT%H:%M:%S"),
                'end': datetime.datetime.fromtimestamp(record['end'] / 1000).strftime("%Y-%m-%dT%H:%M:%S"),
                'mw_cap': str(record['energy_limitation']).replace(',', ''),
                'mw_available': None,
                'status': record['canceled'],
                'pub_ts': datetime.datetime.fromtimestamp(record['publish_timestamp'] / 1000).strftime("%Y-%m-%dT%H:%M:%S"),
                'comment': record['reason']
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
            # try:
            #     error_element = self.driver.find_element_by_xpath('//div[@data-ng-show="noData && !loading && filterActive != false"]')
            #     error_text = error_element.find_element_by_tag_name('p').text
            #     if "There is no data reported"
            print("ERROR: Page load timeout or no data reported.")

            return False


    def parse_current(self, url):
        """
        Parses current availability page.

        :param response: Response object
        :return: Yields availability item dicts.
        """

        self.cur_date = datetime.datetime.utcnow()

        retries = 0
        print("Getting current unavailability page for:")
        print(url)
        self.driver.get(url)

        current_page = 1

        try:
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.XPATH, '//div[@class="timestamp"]')))
        except TimeoutException:
            print("Page load timeout or no data reported.")
            return None

        initial_page = self.driver.page_source
        soup = BeautifulSoup(initial_page)

        # Try to get the page count
        pagecount = 0
        while retries < 3:
            try:
                pagecount = len(soup.find('ul', {'class': 'pager clearfix'}).find_all('li'))
                retries=0
            except AttributeError:
                pagecount=1

            if pagecount == 0:
                print("No data loaded. Retrying...")
                self.driver.refresh()
                retries += 1
                continue
            break

        while current_page <= pagecount:
            if pagecount > 1:
                page_selected = self.select_page(current_page)
                if not page_selected:
                    return None

            time.sleep(1)
            ts_xpath = '//div[@class="timestamp"]'
            try:
                WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.XPATH, ts_xpath)))
            except TimeoutException:
                print("Page load timeout. Retrying.")
                retries += 1
                self.driver.refresh()
                continue

            page_data =self.extract_rows(self.driver.page_source)

            retries = 0
            for item in page_data:
                yield item
            current_page += 1

    def select_page(self, number):
        retries = 0
        newXpath = '//ul[@class="pager clearfix"]/li/a[text()="{0}"]'.format(number)

        page_number_loaded = False

        while retries < 3:
            try:
                print("Waiting page number element: ", number)
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, newXpath)))
                page_number_loaded = True
                break
            except TimeoutException:
                retries += 1
                print("Page load timeout. Try ", retries)
                self.driver.refresh()
                continue

        if not page_number_loaded:
            return False

        page_element = self.driver.find_element_by_xpath(newXpath)

        page_element.click()
        time.sleep(2)
        print('Waiting clicked page...')
        try:
            page_xPath = ('//ul[@class="pager clearfix"]/li/'
                          'a[text()="{0}" and @class="ng-binding active"]').format(number)
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.XPATH, page_xPath)))
            return True
        except TimeoutException:
            print("Page load timeout.")

        print("ERROR: Unable to parse current availabilities.")
        print(self.driver.current_url)
        return False

    def extract_rows(self, page_html):
        """
        Extracts rows from page table to items.
        :param page_html:
        :return: Yields availability items.
        """
        page_soup = BeautifulSoup(page_html)
        table = page_soup.find('table', {'data-table': ''}).find('tbody')
        rows = table.find_all('tr')

        for row in rows:
            data = row.find_all('td')
            try:
                publish_timestamp = datetime.datetime.strptime(data[11].find(text=True).strip(), '%Y/%m/%d %H:%M')
                begin_timestamp   = datetime.datetime.strptime(data[6].find(text=True).strip(), '%Y/%m/%d %H:%M')
                end_timestamp     = datetime.datetime.strptime(data[7].find(text=True).strip(), '%Y/%m/%d %H:%M')
                item = {
                    'parse_date': self.cur_date,
                    'pub_ts'   : publish_timestamp.isoformat(),
                    'type'     : data[0].find(text=True).strip(),
                    'unitname' : data[3].find(text=True).strip(),
                    'area'     : data[5].find(text=True).strip(),
                    'fuel_type': data[4].find(text=True).strip(),
                    'begin'    : begin_timestamp.isoformat(),
                    'end'      : end_timestamp.isoformat(),
                    'mw_cap'   : data[8].find(text=True).strip(),
                    'mw_available': None,
                    'status'   : data[10].find(text=True).strip(),
                    'comment'  : data[9].find(text=True).strip()
                }
                yield item
            except IndexError:
                continue


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

    def load_data(self):
        """Returns Javascript that reloads data on the page."""
        return self._definitions['loadData'] + '\n' + 'loadData();'

    def set_dates(self, fromDate, toDate):
        """Returns JavaScript to set dates interval to load."""
        return self._definitions['setDates'] + '\n'\
               + 'setDates("{0}", "{1}");'.format(fromDate.strftime("%Y-%m-%d"), toDate.strftime("%Y-%m-%d"))
