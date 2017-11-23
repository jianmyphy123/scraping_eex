import csv
import getopt
import sys

import psycopg2

from .config import POSTGRE_CREDENTIALS



class CsvToPostgreMigrator(object):

    pg_credentials = POSTGRE_CREDENTIALS

    def __init__(self, table):
        self.connection = psycopg2.connect(database=self.pg_credentials["database"],
                                           user=self.pg_credentials["user"],
                                           host=self.pg_credentials["host"],
                                           password=self.pg_credentials["password"])
        self.cur = self.connection.cursor()
        self.pending_items = []
        self.failed_items = []
        self.table=table


    def process_item(self, item):
        try:
            self.postgre_upsert(item, self.table)
        except psycopg2.DataError as e:
            print ("ERROR: During save to postgre:", e.pgerror)
            self.connection.rollback()
            self.failed_items.extend(self.pending_items)
            self.pending_items.clear()

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
                            'pub_ts TIMESTAMP,'
                            'status VARCHAR(64),'
                            'unitname VARCHAR(128),'
                            'fuel_type VARCHAR(128),'
                            'begin_ts TIMESTAMP,'
                            'end_ts TIMESTAMP,'
                            'mw_cap DOUBLE PRECISION,'
                            'mw_available DOUBLE PRECISION,'
                            'comment TEXT'
                            ');'
                            ).format(table_name.lower())

            self.cur.execute(create_query)
            self.connection.commit()

    def postgre_upsert(self, item, table_name):
        """Performs an operation similar to 'INSERT OR UPDATE'.

        Not safe for concurrently using multiple siders writing to the same table:
        data loss possible.
        """
        item['table'] = table_name

        update_query = ("UPDATE {0} SET "
                        "type=%(type)s,"
                        "pub_ts=%(pub_ts)s,"
                        "status=%(status)s,"
                        "unitname=%(unitname)s,"
                        "fuel_type=%(fuel_type)s,"
                        "begin_ts=%(begin)s,"
                        "end_ts=%(end)s,"
                        "mw_cap=%(mw_cap)s,"
                        "mw_available=%(mw_available)s,"
                        "comment=%(comment)s"
                        " "
                        "WHERE "
                        "pub_ts<%(pub_ts)s AND "
                        "type=%(type)s AND "
                        "unitname=%(unitname)s AND "
                        "fuel_type=%(fuel_type)s AND "
                        "begin_ts=%(begin)s;"
                        ).format(table_name)
        insert_query = ("INSERT INTO {0} ("
                        "type,"
                        "pub_ts,"
                        "status,"
                        "unitname,"
                        "fuel_type,"
                        "begin_ts,"
                        "end_ts,"
                        "mw_cap,"
                        "mw_available,"
                        "comment"
                        ") "
                        "SELECT "
                        "%(type)s,"
                        "%(pub_ts)s,"
                        "%(status)s,"
                        "%(unitname)s,"
                        "%(fuel_type)s,"
                        "%(begin)s,"
                        "%(end)s,"
                        "%(mw_cap)s,"
                        "%(mw_available)s,"
                        "%(comment)s "
                        "WHERE NOT EXISTS ("
                        "SELECT 1 FROM {0} WHERE "
                        "type=%(type)s AND "
                        "unitname=%(unitname)s AND "
                        "fuel_type=%(fuel_type)s AND "
                        "begin_ts=%(begin)s"
                        ");"
                        ).format(table_name)

        self.pending_items.append(item)
        self.cur.execute(update_query, item)
        self.cur.execute(insert_query, item)


def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[4:], 'i:o:n:vh',
                                   ['in', 'out', 'ignore', 'version', 'help'])
    except getopt.GetoptError as err:
        print(__doc__)
        print(str(err))  # will print something like "option -a not recognized"
        sys.exit(2)


if __name__ == "__main__":
    main(sys.argv[1:])