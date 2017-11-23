# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import shutil
import csv
from zipfile import ZipFile
from tempfile import TemporaryFile
import urllib.parse
import urllib.request
import datetime

import pandas as pd
import psycopg2

from scrapers.config import POSTGRE_CREDENTIALS


class ScrapersPipeline(object):
    def process_item(self, item, spider):
        return item


class RTEFranceFilePipeline(object):

    translate_dict = {
        "status": {
            "Terminée": "closed",
            "Annulée": "cancelled"
        },
        "type": {
            "Indisponibilité planifiée": "planned",
            "Indisponibilité fortuite": "unplanned",

        },
        "fuel_type": {
            "Gaz"      : "Fossil Gas",
            "Fioul"    : "Fossil Oil",
            "Charbon"  : "Fossil Hard coal",
            "Nucléaire": "Nuclear",
            "Hydraulique STEP"        : "Hydro Pumped Storage",
            "Hydraulique fil de l'eau / éclusée": "Hydro Run-of-river and poundage",
            "Hydraulique lacs"        : "Hydro Water Reservoir",
            "Autre": "Other"
        }
    }

    header = ['version', 'type', 'pub_ts', 'status', 'unitname', 'fuel_type',
              'begin', 'end', 'mw_cap', 'mw_available', 'comment']

    def process_item(self, item, spider):
        url = item['file_url']
        with urllib.request.urlopen(url) as response, TemporaryFile(mode='w+b') as out_file:
            shutil.copyfileobj(response, out_file)

            df = self.dataframe_from_archive(out_file)
            params = urllib.parse.parse_qs(getattr(urllib.parse.urlparse(url), 'query'))
            year = params['annee'][0]

            csv_fp = os.path.join(spider.scrape_dir, ''.join(['rte_france_', year, '.csv']))
            df.to_csv(csv_fp, date_format='%Y-%m-%dT%H:%M:%SZ', index=False)

    def translate_cols(self, df, translate_dict):
        for key in translate_dict:
            for variant in translate_dict[key]:
                df.set_value(df[key] == variant,
                             key, translate_dict[key][variant])

    def dataframe_from_archive(self, fileobj):
        with ZipFile(fileobj, mode='r') as archive:
            for name in archive.namelist():
                with archive.open(name) as csv_file, TemporaryFile(mode='w+') as csv_temp:
                    csv_temp.write(csv_file.read().decode('windows-1252'))
                    csv_temp.seek(0)
                    usecols = [1, 3, 4, 5, 8, 9, 11, 13, 14, 16]
                    report_df = pd.read_csv(
                        csv_temp,
                        names=self.header,
                        sep='\t',
                        usecols=usecols,
                        skiprows=1,
                        index_col=False
                    )

                    self.translate_cols(report_df, self.translate_dict)
                    report_df['pub_ts'] = pd.to_datetime(report_df['pub_ts'],
                                                         errors='coerce',
                                                         format='%d/%m/%Y %H:%M')
                    report_df['begin'] = pd.to_datetime(report_df['begin'],
                                                         errors='coerce',
                                                         format='%d/%m/%Y %H:%M')
                    report_df['end'] = pd.to_datetime(report_df['end'],
                                                         errors='coerce',
                                                         format='%d/%m/%Y %H:%M')
                    return report_df


class CSVPipeline(object):
    """This pipeline saves items to corresponding csv files, divided by month"""
    csv_files = {}
    csv_writers = {}
    header = ['type',  'pub_ts', 'status', 'unitname',     'fuel_type',
              'begin', 'end',    'mw_cap', 'mw_available', 'comment']

    def open_spider(self, spider):
        self.csv_files[spider] = {}
        self.csv_writers[spider] = {}

    def close_spider(self, spider):
        for file in self.csv_files[spider].values():
            try:
                file.close()
            except Exception as e:
                print("Exception on closing file:")
                print(e)

    def process_item(self, item, spider):
        filename = os.path.join(spider.scrape_dir,
                                ''.join([spider.name, item['parse_date'].strftime('%Y-%m'), '.csv']))
        try:
            self.csv_writers[spider][filename].writerow(item)
        except KeyError:
            print("File does not exist. Creating file...")
            self.csv_files[spider][filename] = open(filename, mode='a')
            print("File ", self.csv_files[spider][filename], " created.")
            self.csv_writers[spider][filename] = csv.DictWriter(self.csv_files[spider][filename],
                                                                fieldnames=self.header,
                                                                extrasaction='ignore')
            self.csv_writers[spider][filename].writeheader()
            self.csv_writers[spider][filename].writerow(item)

        return item


class PostgrePipeline(object):
    """This pipeline saves data to PostgreSQL database.

    Credentials to connect to database are stored in config.py,
    POSTGRE_CREDENTIALS variable.
    """
    pg_credentials = POSTGRE_CREDENTIALS
    header = ['type', 'pub_ts', 'status', 'unitname', 'fuel_type',
              'begin', 'end', 'mw_cap', 'mw_available', 'comment']

    def __init__(self):
        self.connection = psycopg2.connect(database=self.pg_credentials["database"],
                                           user=self.pg_credentials["user"],
                                           host=self.pg_credentials["host"],
                                           password=self.pg_credentials["password"])
        self.cur = self.connection.cursor()
        self.pending_items = []
        self.failed_items = []

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

        # try:
        #     self.update_latest(table_name=spider.table)
        # except Exception as e:
        #     print("Failed to set lates items")
        #     print(e)
        #     self.connection.rollback()
        #     raise e
        # finally:
        #     self.connection.close()

        if self.failed_items:
            print("FAILED ITEMS:")
            for failed in self.failed_items:
                print(failed)

        print("FAILED ITEMS:")
        for item in self.failed_items:
            print(item)

    def process_item(self, item, spider):

        retries = 1

        for field in self.header:
            if field not in item:
                item['field']=None
        while retries < 3:
            try:
                self.postgre_upsert(item, spider.table)
                break
            except (psycopg2.DataError, psycopg2.IntegrityError) as e:
                print ("ERROR: During save to postgre:", e.pgerror)
                self.connection.rollback()
                self.failed_items.extend(self.pending_items)
                self.pending_items.clear()
                break
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
            create_query = ('CREATE TABLE IF NOT EXISTS {0}('
                            'id BIGSERIAL PRIMARY KEY,'
                            'type VARCHAR(64),'
                            'pub_ts TIMESTAMP WITH TIME ZONE,'
                            'status VARCHAR(16),'
                            'unitname VARCHAR(128),'
                            'area VARCHAR(128),'
                            'fuel_type VARCHAR(128),'
                            'begin_ts TIMESTAMP WITH TIME ZONE,'
                            'end_ts TIMESTAMP WITH TIME ZONE,'
                            'mw_cap DOUBLE PRECISION,'
                            'mw_available DOUBLE PRECISION,'
                            'comment TEXT,'
                            'last_availability BOOLEAN '
                            ');'
                            ).format(table_name.lower())

            self.cur.execute(create_query)
            self.connection.commit()

    def postgre_upsert(self, item, table_name):
        # Performs an operation similar to 'INSERT OR UPDATE'.
        #
        # Not safe for concurrently using multiple siders writing to the same table:
        # data loss possible.

        # item['table'] = table_name

        insert_query = ("INSERT INTO {0} ("
                        "type,"
                        "pub_ts,"
                        "status,"
                        "unitname,"
                        "area,"
                        "fuel_type,"
                        "begin_ts,"
                        "end_ts,"
                        "mw_cap,"
                        "mw_available,"
                        "comment"
                        ") "
                        "VALUES ("
                        "%(type)s,"
                        "%(pub_ts)s,"
                        "%(status)s,"
                        "%(unitname)s,"
                        "%(area)s,"
                        "%(fuel_type)s,"
                        "%(begin)s,"
                        "%(end)s,"
                        "%(mw_cap)s,"
                        "%(mw_available)s,"
                        "%(comment)s);"
                        ).format(table_name)

        self.pending_items.append(item)
        self.cur.execute(insert_query, item)

    def update_latest(self, table_name):
        """Queries through the database to set the 'latest_availability' flag."""

        unique_unav_query = "SELECT DISTINCT unitname, fuel_type FROM {0};".format(table_name)
        self.cur.execute(unique_unav_query)
        self.connection.commit()

        time_now = datetime.datetime.utcnow().isoformat()

        max_subquery_template = ("(SELECT MAX(pub_ts) FROM {0} WHERE "
                        "unitname=%(unitname)s AND "
                        "fuel_type=%(fuel_type)s AND "
                        "NOT begin_ts>{1})"
                        ).format(table_name, time_now)

        update_query_template = ("UPDATE {0} SET "
                                 "latest_availability=1"
                                 " "
                                 "WHERE "
                                 "unitname=%(unitname)s AND "
                                 "fuel_type=%(fuel_type)s AND "
                                 "pub_ts=%(subquery)s;"
                                 ).format(table_name)

        for record in self.cur.fetchall():
            try:
                rec_dict = {'fuel_type':record[1], 'unitname': record[0],}
                max_subquery = self.cur.mogrify(max_subquery_template,
                                                rec_dict)
            except Exception as e:
                print("Bad record:", type(record), record)
                for it in record:
                    print(it)
                raise e
            else:
                rec_dict['subquery'] = max_subquery

                try:
                    self.cur.execute(update_query_template, rec_dict)
                except Exception as e:
                    print("QUERY: ", max_subquery)
                    raise e
