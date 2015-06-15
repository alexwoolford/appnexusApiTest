#!/usr/bin/env python
import requests
import urllib
import json
import ConfigParser
import MySQLdb

config = ConfigParser.RawConfigParser()
config.read('appnexus-default.properties')


class AppNexusBasicStats:
    """Gets AppNexus basic stats (impressions and revenue) and writes them to a MySQL table"""

    def __init__(self):

        # The possible named report intervals are documented here: https://wiki.appnexus.com/display/api/Report+Service
        # Search on the page for "report_interval"
        self.report_interval = "today"

        self.authenticate()
        self.connectToDB()
        self.createTable()
        self.createReportChunkUrls()
        self.makeCallsAndWriteToDB()

    def authenticate(self):
        self.s = requests.session()
        credentials = json.dumps(
            {'auth': {'username': config.get('appnexus', 'username'), 'password': config.get('appnexus', 'password')}})
        self.s.post(url="https://api.appnexus.com/auth", data=credentials)

    def connectToDB(self):
        self.con = MySQLdb.connect(host=config.get('database', 'host'), db=config.get('database', 'db'),
                                   user=config.get('database', 'username'), passwd=config.get('database', 'password'))

    def createTable(self):
        sql = "DROP TABLE IF EXISTS appnexus_basic_stats"
        self.cursor = self.con.cursor()
        self.cursor.execute(sql)
        sql = "CREATE TABLE appnexus_basic_stats (publisher_id int, publisher_name varchar(255), impressions bigint, revenue decimal(15, 5))"
        self.cursor.execute(sql)

    def createReportChunkUrls(self):
        response = self.s.get(url="http://api.appnexus.com/publisher")
        response_count = json.loads(response.content)['response']['count']
        start_elements = [start_element for start_element in range(0, response_count, 100)]
        self.urls = []
        for start_element in start_elements:
            url = "http://api.appnexus.com/publisher?" + urllib.urlencode(
                {"state": "active", "interval": self.report_interval, "sort": "imps.desc", "stats": "true",
                 "object_status": "true", "is_rtb": "false", "start_element": start_element, "num_elements": "100"})
            self.urls.append(url)

    def makeCallsAndWriteToDB(self):
        for url in self.urls:
            response = self.s.get(url=url)

            for publisher in json.loads(response.content)['response']['publishers']:

                id = publisher['id']
                name = publisher['name']
                imps = publisher['stats']['imps']
                revenue = publisher['stats']['revenue']

                if imps == None:
                    imps = 0

                if revenue == None:
                    revenue = 0

                sql = """INSERT INTO appnexus_basic_stats (publisher_id, publisher_name, impressions, revenue) VALUES ({0}, {1}, {2}, {3})""".format(
                    id, json.dumps(name), imps, revenue)
                self.cursor.execute(sql)

        self.cursor.execute('commit')

if __name__ == "__main__":
    appNexusBasicStats = AppNexusBasicStats()
