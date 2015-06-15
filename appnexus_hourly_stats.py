#!/usr/bin/env python
import requests
import urllib
import json
import ConfigParser
import MySQLdb
import time

config = ConfigParser.RawConfigParser()
config.read('appnexus-default.properties')


class AppNexusHourlyStats:
    """Gets AppNexus hourly stats for each publisher ID (impressions and revenue) and writes them to a MySQL table"""

    def __init__(self):
        self.connectToDB()
        self.getPublisherIds()
        self.authenticate()
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

    def writeHourlyDataToDB(self):
        for publisher_id in self.publisher_ids[:1]:
            report_json = json.dumps(eval("""{{   "report": {{
                                                        "row_per": [
                                                            "hour"
                                                        ],
                                                        "report_interval": "last_48_hours",
                                                        "timezone": "EST5EDT",
                                                        "columns": [
                                                            "hour",
                                                            "imps_total",
                                                            "imps_sold",
                                                            "clicks",
                                                            "network_revenue",
                                                            "publisher_revenue",
                                                            "total_network_rpm",
                                                            "sold_network_rpm"
                                                        ],
                                                        "orders": [
                                                            "hour"
                                                        ],
                                                        "report_type": "network_publisher_analytics",
                                                        "filters": [
                                                            {{
                                                                "publisher_id": "{publisher_id}"
                                                            }},
                                                            {{
                                                                "member_id": "982"
                                                            }},
                                                            {{
                                                                "seller_member_id": "982"
                                                            }},
                                                            {{
                                                                "imp_type": [
                                                                    "Resold"
                                                                ]
                                                            }}
                                                        ]
                                                    }}
                                                }}""".format(publisher_id=publisher_id)))

            response = self.s.post("http://api.appnexus.com/report?publisher_id={publisher_id}".format(publisher_id=publisher_id), data=report_json)
            report_id = json.loads(response.content)['response']['report_id']
            time.sleep(1)
            report = self.s.get("http://api.appnexus.com/report-download?" + urllib.urlencode({'id': report_id}))
            print(report.content)
            # print("debug breakpoint")


if __name__ == "__main__":
    appNexusHourlyStats = AppNexusHourlyStats()
