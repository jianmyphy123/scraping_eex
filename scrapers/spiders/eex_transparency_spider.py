import scrapy
import datetime
import time
import pytz
import json

from selenium.common import exceptions as selenium_exceptions
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import calendar

class EexTransparencySpider(scrapy.Spider):
    name = 'eex_transparency'

    # the urls to fetch date range data
    history_url_list = [
                        'https://www.eex-transparency.com/homepage/power/austria/production/availability/non-usability-/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/belgium/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/switzerland/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/czech-republic/production/availability/non-usability/non-usability',
                        'https://www.eex-transparency.com/homepage/power/germany/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/great-britain/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/hungary/production/availability/non-usability/non-usability-history',
                        'https://www.eex-transparency.com/homepage/power/italy/production/availability/non-usability/non-usability-history-',
                        'https://www.eex-transparency.com/homepage/power/the-netherlands/production/availability/non-usability/non-usability-history-'
                        ]
    
    history_url_list_dict = {
        "austria": 'https://www.eex-transparency.com/homepage/power/austria/production/availability/non-usability-/non-usability-history-',
        "belgium": 'https://www.eex-transparency.com/homepage/power/belgium/production/availability/non-usability/non-usability-history-',
        "switzerland": 'https://www.eex-transparency.com/homepage/power/switzerland/production/availability/non-usability/non-usability-history-',
        "czech-republic": 'https://www.eex-transparency.com/homepage/power/czech-republic/production/availability/non-usability/non-usability',
        "germany": 'https://www.eex-transparency.com/homepage/power/germany/production/availability/non-usability/non-usability-history-',
        "great-britain": 'https://www.eex-transparency.com/homepage/power/great-britain/production/availability/non-usability/non-usability-history-',
        "hungary": 'https://www.eex-transparency.com/homepage/power/hungary/production/availability/non-usability/non-usability-history',
        "italy": 'https://www.eex-transparency.com/homepage/power/italy/production/availability/non-usability/non-usability-history-',
        "the-netherlands": 'https://www.eex-transparency.com/homepage/power/the-netherlands/production/availability/non-usability/non-usability-history-'
    }

    # the urls to fetch recent data (today and yesterday)
    recent_url_list = ['https://www.eex-transparency.com/homepage/power/austria/storage/availability/non-usability',
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
            'scrapers.pipelines.PostgrePipeline': 500
        }
    }

    def __init__(self, mode='recent', period=None, country=None):
        super().__init__()
        
        if mode == 'history':
            if period == None:
                print('Parameter error')
            else:
                self.period = period
                
                period = period.split('-')
                if len(period) != 2:
                    print('Parameter error')
                else:
                    year = period[0]
                    month = period[1]
                    start = '-'.join([year, month, "1"])
                    end = '-'.join([year, month, str(calendar.monthrange(int(year), int(month))[1])])                    
                    self.start      = datetime.datetime.strptime(start, '%Y-%m-%d')
                    self.end        = datetime.datetime.strptime(end, '%Y-%m-%d')
                    
            if country is not None:
                if country not in self.history_url_list_dict.keys():
                    print('Parameter error')
                else:
                    self.history_url_list = [self.history_url_list_dict[country]]
                

        now_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        self.now_date   = datetime.datetime.strptime(now_date, '%Y-%m-%d')

        # postgre database table name definition
        self.table = self.name
            
        self.mode = mode
        self.scraper = ScrapeJS()

        self.driver = webdriver.PhantomJS('./phantomjs/linux/phantomjs')
        # self.driver = webdriver.Chrome('./chromedriver')

        # setting log file
        self.log_file_name = 'logs/' + datetime.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S") + '.log'
        self.item_scraped_count = 0
        with open(self.log_file_name, mode='w+') as log_file:
            if mode == 'history':
                self.scrape_info = {
                    'start_date': start,
                    'end_date': end,
                    'item_scraped_count': 0,
                    'failed_data': {}
                }
            elif mode == 'recent':
                self.scrape_info = {
                    'start_date': now_date,
                    'end_date': now_date,
                    'item_scraped_count': 0,
                    'failed_data': {}
                }
            json.dump(self.scrape_info, log_file, indent=4)

    def start_requests(self):
        yield scrapy.Request('https://www.eex-transparency.com/', callback=self.start_requests_selenium)

    def start_requests_selenium(self, response):
        print("Connection OK. Start scraping...")

        if self.mode == 'recent':
            print('Start scraping with recent mode...')
            for url in self.recent_url_list:
                print("Recent for country: ", url)
                items = self.parse_recent(url)
                if items is None:
                    yield
                else:
                    for item in items:
                        yield item

        elif self.mode == 'history':
            print('Start scraping with history mode...')

            for url in self.history_url_list:
                print("History for country: ", url)
                items = self.parse_history(url)
                if items is None:
                    yield
                else:
                    for item in items:
                        yield item

        else:
            print('Parameter Error.')
            yield
                
    def parse_history(self, url):
        self.driver.get(url)
        
        first_page_loaded = self._load_page(self.start, self.end, url)
        
        if first_page_loaded:
            pass
        else:
            print("Unable to load page. Skipping.")
            yield
        
        print("[*] Parsing page")

        data_object = self.driver.execute_script(self.scraper.get_history_table_data())
        items = self.parse_data_object(data_object)
        if items is None:
            print('Items not found in that url: ', url)
            yield
        else:
            for item in items:
                yield item
            
            check_next_page = self.driver.execute_script(self.scraper.check_next_page())
            if check_next_page:
                self.driver.execute_script(self.scraper.load_next_page())
                items = self.parse_history_details(url)
                
                if items is None:
                    print('Items not found in that url: ', url)
                    # log file
                    yield
                else:
                    for item in items:
                        yield item
    
    def parse_history_details(self, url):
        page_loaded = self._load_page_history(url)
        
        if page_loaded:
            pass
        else:
            print("Unable to load page. Skipping.")
            yield
        
        data_object = self.driver.execute_script(self.scraper.get_history_table_data())
        items = self.parse_data_object(data_object)
        if items is None:
            # print('Items not found in that url: ', url)
            yield
        else:
            for item in items:
                yield item
            
            check_next_page = self.driver.execute_script(self.scraper.check_next_page())
            
            if check_next_page:
                self.driver.execute_script(self.scraper.load_next_page())
                items = self.parse_history_details(url)
                
                if items is None:
                    print('Items not found in that url: ', url)
                    # log file
                    yield
                else:
                    for item in items:
                        yield item

    def parse_recent(self, url):
        self.driver.get(url)

        print('[*] Loading page')
        page_loaded = self._load_page(self.now_date, self.now_date, url)

        if page_loaded:
            pass
        else:
            print("Unable to load page. Skipping.")
            return

        print("[*] Parsing page")
        data_object = self.driver.execute_script(self.scraper.get_recent_table_data())

        items = self.parse_data_object(data_object)
        if items is None:
            print('Items not found in that url: ', url)
            yield
        else:
            for item in items:
                yield item


    def _load_page(self, start, end, url):
        """
        Loads particular date.

        :param date: Date to load
        :return: Returns True on success.
        """

        if self.mode == 'history':
            print("---- Loading date: ", start, ' ', end, ' ----')

            try:
                self.driver.execute_script(self.scraper.set_dates(start, end))
            except selenium_exceptions.WebDriverException as e:
                print("LOAD DATE ERROR:", e.msg)
                self._log_failed_data(self.period, url)
                return False


        self.driver.refresh()

        # check that page loaded
        try:
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.XPATH, "//div[@class='timestamp']")))
            return True
        except TimeoutException:
            # check that data is empty
            isEmpty = self.driver.execute_script(self.scraper.is_empty_table_data())
            if isEmpty:
                print('There is no data reported.')
                return True

            print("ERROR: Page load timeout.")
            if self.mode == 'history':
                self._log_failed_data(self.period, url)
            elif self.mode == 'recent':
                self._log_failed_data(start, url)
            return False
        
    def _load_page_history(self, url):
        try:
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.XPATH, "//div[@class='timestamp']")))
            return True
        except TimeoutException:
            print("ERROR: Page load timeout.")
            self._log_failed_data(self.period, url)
            return False

    def _log_failed_data(self, date, url):
        with open(self.log_file_name, mode='w+') as log_file:
            if self.mode == 'recent':
                failed_date_str = date.strftime('%Y-%m-%d')
            else:
                failed_date_str = date
               
            if failed_date_str not in self.scrape_info['failed_data'].keys():
                self.scrape_info['failed_data'][failed_date_str] = []
                self.scrape_info['failed_data'][failed_date_str].append(url)
            else:
                self.scrape_info['failed_data'][failed_date_str].append(url)
            json.dump(self.scrape_info, log_file, indent=4)


    def parse_data_object(self, data_object):
        """
        Parses data object to items
        :param data_object: Source data object.
        :param parse_date: Parse date for the pipeline.
        :return: Yields availability item.
        """
        if data_object is None:
            yield
        else:
            for record in data_object:
                item = {
                    'type': record['type'],
                    'company': record['short_name'],
                    'facility': record['prodcon'],
                    'unit': record['unit'],
                    'fuel': record['fuel'] if 'fuel' in record.keys() else "" ,
                    'control_area': record['connecting_area'],
                    'begin_ts': datetime.datetime.fromtimestamp(record['begin'] / 1000, tz=pytz.timezone('CET')).strftime("%Y-%m-%dT%H:%M:%S"),
                    'end_ts': datetime.datetime.fromtimestamp(record['end'] / 1000, tz=pytz.timezone('CET')).strftime("%Y-%m-%dT%H:%M:%S"),
                    'limitation': record['energy_limitation'],
                    'reason': record['reason'],
                    'status': record['canceled'],
                    'event_id': record['event_id'],
                    'last_update': datetime.datetime.fromtimestamp(record['modify_timestamp'] / 1000, tz=pytz.timezone('CET')).strftime("%Y-%m-%dT%H:%M:%S")
                }
                self.item_scraped_count += 1
                yield item

class ScrapeJS(object):
    """A helper class that generates javascript snippets to use with browser."""
    def __init__(self):
        self._definitions = {
            'getHistoryTableData':
                ('function getHistoryTableData() {\n'
                    'var e = document.getElementById("from");\n'
                    'var sc = angular.element(e).scope();\n'
                    'var rows_ng = sc.eventData;\n'
                    'return rows_ng;\n'
                '}\n'
                ),
            'getRecentTableData':
                ('function getRecentTableData() {\n'
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
                    'console.log(document.body.innerHTML);\n'
                    'var from = moment(fromDate);\n'
                    'var to = moment(tDate);\n'
                    'sc.to = to.toDate();\n'
                    'sc.$apply();\n'
                    '$("#from").blur();\n'
                    'sc.from = from.toDate();\n'
                    'sc.$apply();\n'
                    '$("#from").blur();\n'
                    'sc.canceled = "all";\n'
                    'sc.selectCanceled();\n'
                 '}\n'
                ),
            'isEmptyTableData':
                ('function isEmptyTableData() {\n'
                    'var e = document.querySelectorAll(\'[data-ng-show="noData && !loading && filterActive != false"]\');\n'
                    'var classList = e[0].classList\n'
                    'return !classList.contains("ng-hide");\n'
                '}\n'
                ),
            'checkNextPage':
                ('function checkNextPage() {\n'
                    'var e = document.getElementsByClassName("next");\n'
                    'var classList = e[0].classList\n'
                    'return !classList.contains("ng-hide");\n'
                '}\n'
                ),
            'loadNextPage':
                ('function loadNextPage() {\n'
                    'var e = document.getElementById("from");\n'
                    'var sc = angular.element(e).scope();\n'
                    'sc.next();\n'
                '}\n'
                ),
        }

    def get_history_table_data(self):
        """Returns JavaScript to get history table data."""
        return self._definitions['getHistoryTableData'] + '\n' + 'return getHistoryTableData();'

    def get_recent_table_data(self):
        """Returns JavaScript to get current table data."""
        return self._definitions['getRecentTableData'] + '\n' + 'return getRecentTableData();'

    def set_dates(self, fromDate, toDate):
        """Returns JavaScript to set dates interval to load."""
        return self._definitions['setDates'] + '\n'\
               + 'setDates("{0}", "{1}");'.format(fromDate.strftime("%Y-%m-%d"), toDate.strftime("%Y-%m-%d"))

    def is_empty_table_data(self):
        """Returns JavaScript to check if loaded data is empty."""
        return self._definitions['isEmptyTableData'] + '\n' + 'return isEmptyTableData();'
        
    def check_next_page(self):
        """Returns JavaScript to check if next page exist."""
        return self._definitions['checkNextPage'] + '\n' + 'return checkNextPage();'
    
    def load_next_page(self):
        """Returns JavaScript to get next page."""
        return self._definitions['loadNextPage'] + '\n' + 'return loadNextPage();'
