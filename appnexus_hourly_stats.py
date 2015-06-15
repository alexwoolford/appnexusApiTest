__author__ = 'awoolford'

#!/usr/bin/env python
import requests
import urllib
import json
import ConfigParser
import MySQLdb

config = ConfigParser.RawConfigParser()
config.read('appnexus-default.properties')

class AppNexusHourlyStats:
    """Gets AppNexus hourly stats for each publisher ID (impressions and revenue) and writes them to a MySQL table"""

    def __init__(self):
        self.connectToDB()
        self.getActivePublisherIds()
        print(self.publisher_ids)

    def connectToDB(self):
        self.con = MySQLdb.connect(host=config.get('database', 'host'), db=config.get('database', 'db'),
                                   user=config.get('database', 'username'), passwd=config.get('database', 'password'))

    def getActivePublisherIds(self):
        sql = "SELECT publisher_id FROM {0}.appnexus_basic_stats where impressions > 0".format(config.get('database', 'db'))
        cursor = self.con.cursor()
        cursor.execute(sql)
        self.publisher_ids = [publisher_id[0] for publisher_id in cursor.fetchall()]

if __name__ == "__main__":
    appNexusHourlyStats = AppNexusHourlyStats()