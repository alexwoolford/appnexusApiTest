#!/usr/bin/env python
from collections import OrderedDict
import requests
import urllib
import json
import ConfigParser
import MySQLdb
import time
import sys

config = ConfigParser.RawConfigParser()
config.read('appnexus-default.properties')


class AppNexusHourlyStats:
    """Gets AppNexus hourly stats for each publisher ID (impressions and revenue) and writes them to a MySQL table"""

    def __init__(self):
        self.connectToDB()
        self.getPublisherIds()
        self.authenticate()
        self.defineColumns()
        self.createTable()
        self.writeHourlyDataToDB()

    def connectToDB(self):
        self.con = MySQLdb.connect(host=config.get('database', 'host'), db=config.get('database', 'db'),
                                   user=config.get('database', 'username'), passwd=config.get('database', 'password'))

    def getPublisherIds(self):
        sql = "SELECT publisher_id FROM {0}.appnexus_basic_stats where impressions > 0".format(
            config.get('database', 'db'))
        cursor = self.con.cursor()
        cursor.execute(sql)
        self.publisher_ids = [publisher_id[0] for publisher_id in cursor.fetchall()]

    def authenticate(self):
        self.s = requests.session()
        credentials = json.dumps(
            {'auth': {'username': config.get('appnexus', 'username'), 'password': config.get('appnexus', 'password')}})
        self.s.post(url="https://api.appnexus.com/auth", data=credentials)

    def defineColumns(self):
        self.column_dict = \
            OrderedDict({1:("hour", "datetime"),
                         2:("imps_total", "int"),
                         3:("imps_sold", "int"),
                         4:("clicks", "int"),
                         5:("network_revenue", "Decimal(15,5)"),
                         6:("publisher_revenue", "Decimal(15,5)"),
                         7:("total_network_rpm", "double"),
                         8:("sold_network_rpm", "double")})

        self.columns_quoted = '"' + '", \n"'.join([colname for colname, coltype in self.column_dict.values()]) + '"'
        self.columns_unquoted = ', '.join([colname for colname, coltype in self.column_dict.values()])

    def createTable(self):
        cursor = self.con.cursor()
        sql = "DROP TABLE IF EXISTS appnexus_detailed_stats"
        cursor.execute(sql)
        sql = "CREATE TABLE appnexus_detailed_stats (publisher_id int, " + ", \n".join([colname + " " + datatype for colname, datatype in self.column_dict.values()]) + ")"
        cursor.execute(sql)

    def writeHourlyDataToDB(self):
        for publisher_id in self.publisher_ids:

            try:
                report_json = json.dumps(eval("""{{   "report": {{
                                                            "row_per": [
                                                                "hour"
                                                            ],
                                                            "report_interval": "last_48_hours",
                                                            "timezone": "EST5EDT",
                                                            "columns": [
                                                                {columns}
                                                            ],
                                                            "orders": [
                                                                "hour"
                                                            ],
                                                            "report_type": "network_publisher_analytics",
                                                            "filters": [
                                                                {{
                                                                    "publisher_id": {publisher_id}
                                                                }},
                                                                {{
                                                                    "member_id": 982
                                                                }},
                                                                {{
                                                                    "seller_member_id": 982
                                                                }},
                                                                {{
                                                                    "imp_type": [
                                                                        "Resold"
                                                                    ]
                                                                }}
                                                            ]
                                                        }}
                                                    }}""".format(columns=self.columns_quoted, publisher_id=publisher_id)))

                response = self.s.post("http://api.appnexus.com/report?publisher_id={publisher_id}".format(publisher_id=publisher_id), data=report_json)
                report_id = json.loads(response.content)['response']['report_id']
                time.sleep(2)
                report = self.s.get("http://api.appnexus.com/report-download?" + urllib.urlencode({'id': report_id}))

                cursor = self.con.cursor()

                for record in report.content.strip().split("\n")[1:]:

                    # TODO: make the SQL insert statement dynamically generate from the defined columns.

                    hour, imps_total, imps_sold, clicks, network_revenue, publisher_revenue, total_network_rpm, sold_network_rpm = [elem.strip() for elem in record.split(",")]

                    sql = """INSERT INTO appnexus_detailed_stats ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}) """.format("publisher_id", "hour", "imps_total", "imps_sold", "clicks", "network_revenue", "publisher_revenue", "total_network_rpm", "sold_network_rpm") + \
                          """VALUES ({0}, '{1}', {2}, {3}, {4}, {5}, {6}, {7}, {8})""".format(publisher_id, hour, imps_total, imps_sold, clicks, network_revenue, publisher_revenue, total_network_rpm, sold_network_rpm)

                    cursor.execute(sql)

                cursor.execute('commit')

            except:
                print("Error processing publisher_id " + str(publisher_id) + "; report_id " + report_id)
                print(sys.exc_info())

if __name__ == "__main__":
    appNexusHourlyStats = AppNexusHourlyStats()
