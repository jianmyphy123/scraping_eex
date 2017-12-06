# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
import datetime
import psycopg2

from scrapers.config import POSTGRE_CREDENTIALS

# save item to Postgre
class PostgrePipeline(object):
    """This pipeline saves data to PostgreSQL database.

    Credentials to connect to database are stored in config.py,
    POSTGRE_CREDENTIALS variable.
    """
    pg_credentials = POSTGRE_CREDENTIALS
    schema = 'covalis1'

    # connect to Postgre
    def __init__(self):
        self.connection = psycopg2.connect(database=self.pg_credentials["database"],
                                           user=self.pg_credentials["user"],
                                           host=self.pg_credentials["host"],
                                           password=self.pg_credentials["password"])
        self.cur = self.connection.cursor()
        self.pending_items = []
        self.failed_items = []
        self.event_ids = []
        self.db_inserted_item_count = 0
        self.db_passed_item_count = 0

    # create table to save data
    def open_spider(self, spider):
        self.create_table(spider.table)

    def close_spider(self, spider):

        try:
            self.connection.commit()
            self.pending_items.clear()

        except psycopg2.DataError as e:
            self.connection.rollback()
            self.failed_items.extend(self.pending_items)
            self.pending_items.clear()
            print("ERROR: During save to postgre:", e.pgerror)

        if self.failed_items:
            print("FAILED ITEMS:")
            for failed in self.failed_items:
                print(failed)

        # update version number for every collected event ids when spider is closing
        for event_id in self.event_ids:
            self.update_version_no(spider.table, event_id)

        self.event_ids.clear()

        # save item_scraped_count to log file
        with open(spider.log_file_name, mode='w+') as log_file:
            spider.scrape_info['item_scraped_count'] = spider.item_scraped_count
            spider.scrape_info['db_inserted_item_count'] = self.db_inserted_item_count
            spider.scrape_info['db_passed_item_count'] = self.db_passed_item_count
            json.dump(spider.scrape_info, log_file, indent=4)

    # save item to Postgre
    def process_item(self, item, spider):

        try:
            self.postgre_upsert(item, spider.table)

        except (psycopg2.DataError, psycopg2.IntegrityError) as e:
            print ("ERROR: During save to postgre:", e.pgerror)
            self.connection.rollback()
            self.failed_items.extend(self.pending_items)
            self.pending_items.clear()
        except psycopg2.DatabaseError:
            self.connection = psycopg2.connect(database=self.pg_credentials["database"],
                                               user=self.pg_credentials["user"],
                                               host=self.pg_credentials["host"],
                                               password=self.pg_credentials["password"])
            self.cur = self.connection.cursor()
            self.postgre_upsert(item, spider.table)

        if len(self.pending_items) > 10:
            try:
                self.connection.commit()
            except psycopg2.DataError as e:
                self.connection.rollback()
                self.failed_items.extend(self.pending_items)
                print("ERROR: During save to postgre:", e.pgerror)
            except Exception as e:
               self.connection.rollback()
               self.failed_items.extend(self.pending_items)
               print("ERROR: Unexpected error during save to postgre:", e)
            finally:
                self.pending_items.clear()


        return item

    def create_table(self, table_name):
        """Creates a table to hold the data if it does not exist."""
        self.cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)", (table_name,))

        if not (self.cur.fetchone()[0]):
            create_query = ('CREATE TABLE IF NOT EXISTS {0}.{1}('
                            'id BIGSERIAL PRIMARY KEY,'
                            'type VARCHAR(64),'
                            'company VARCHAR(64),'
                            'facility VARCHAR(64),'
                            'unit VARCHAR(128),'
                            'fuel VARCHAR(128),'
                            'control_area VARCHAR(128),'
                            'begin_ts TIMESTAMP,'
                            'end_ts TIMESTAMP,'
                            'limitation DOUBLE PRECISION,'
                            'reason TEXT,'
                            'status VARCHAR(16),'
                            'event_id VARCHAR(128),'
                            'last_update TIMESTAMP,'
                            'version_no INTEGER DEFAULT 1 '
                            ');'
                            ).format(self.schema, table_name.lower())

            self.cur.execute(create_query)
            self.connection.commit()

    def postgre_upsert(self, item, table_name):
        # check if duplicated item exists
        item_exists_query = ("select id from {0}.{1} "
                                    "WHERE "
                                    "event_id = "
                                    "%(event_id)s "
                                    "and begin_ts = "
                                    "%(begin_ts)s "
                                    "and end_ts = "
                                    "%(end_ts)s "
                                    "and last_update = "
                                    "%(last_update)s "
                                    ).format(self.schema, table_name)
        self.cur.execute(item_exists_query, {'event_id': item['event_id'], 'begin_ts': item['begin_ts'],
                                             'end_ts': item['end_ts'], 'last_update': item['last_update']})
        rows = self.cur.fetchall()

        if len(rows) > 0:
            self.db_passed_item_count += 1
            pass
        else:
            insert_query = ("INSERT INTO {0}.{1} ("
                            "type,"
                            "company,"
                            "facility,"
                            "unit,"
                            "fuel,"
                            "control_area,"
                            "begin_ts,"
                            "end_ts,"
                            "limitation,"
                            "reason,"
                            "status,"
                            "event_id,"
                            "last_update"
                            ") "
                            "VALUES ("
                            "%(type)s,"
                            "%(company)s,"
                            "%(facility)s,"
                            "%(unit)s,"
                            "%(fuel)s,"
                            "%(control_area)s,"
                            "%(begin_ts)s,"
                            "%(end_ts)s,"
                            "%(limitation)s,"
                            "%(reason)s,"
                            "%(status)s,"
                            "%(event_id)s,"
                            "%(last_update)s);"
                            ).format(self.schema, table_name)

            self.pending_items.append(item)
            self.cur.execute(insert_query, item)

            self.db_inserted_item_count += 1

        # collect event_ids to update
        if item["event_id"] not in self.event_ids:
            self.event_ids.append(item['event_id'])

    # the function to update version number
    def update_version_no(self, table_name, event_id):
        events_query = ("select id from {0}.{1} "
                        "where event_id="
                        "%(event_id)s "
                        "order by last_update;"
                        ).format(self.schema, table_name)
        self.cur.execute(events_query, {'event_id': event_id})
        rows = self.cur.fetchall()

        version_no = 1
        for row in rows:
            id = row[0]
            update_version_no_query = ("UPDATE {0}.{1} SET "
                                        "version_no = "
                                        "%(version_no)s "
                                        "WHERE "
                                        "id = "
                                        "%(id)s"
                                        ).format(self.schema, table_name)
            self.cur.execute(update_version_no_query, {'version_no': version_no, 'id': id})
            version_no += 1

        self.connection.commit()
