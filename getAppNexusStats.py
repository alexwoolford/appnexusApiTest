#!/usr/bin/env python
import requests
import urllib
import json
import ConfigParser
import MySQLdb
config = ConfigParser.RawConfigParser()
config.read('appnexus-default.properties')

class AppNexusBasicStats:
    """AppNexus basic stats"""
    def __init__(self):
        self.authenticate()

    def authenticate(self):
        self.s = requests.session()
        credentials = json.dumps({'auth': {'username' : config.get('appnexus', 'username'), 'password' : config.get('appnexus', 'password')}})
        self.s.post(url = "https://api.appnexus.com/auth", data = credentials)

    def createReportChunkUrls(self):
        self.urls = []
        for start_element in [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500]:
            url = "http://api.appnexus.com/publisher?" + urllib.urlencode({"state":"active",  "interval":"lifetime", "sort":"imps.desc", "stats":"true", "object_status":"true", "is_rtb":"false", "start_element":start_element, "num_elements":"100"})
            self.urls.append(url)

    def connectToDB(self):
        self.con = MySQLdb.connect(host=config.get('database', 'host'), db=config.get('database', 'db'), user = config.get('database', 'username'), passwd = config.get('database', 'password'))

    def createTable(self):
        sql = "DROP TABLE IF EXISTS appnexus_basic_stats"
        self.cursor = self.con.cursor()
        self.cursor.execute(sql)
        sql = "CREATE TABLE appnexus_basic_stats (publisher_id int, publisher_name varchar(255), impressions bigint, revenue decimal(15, 5))"
        self.cursor.execute(sql)

    def makeCallsAndWriteToDB(self):
        for url in self.urls:
            response = self.s.get(url = url)
            for publisher in json.loads(response.content)['response']['publishers']:

                id = publisher['id']
                name = publisher['name']
                imps = publisher['stats']['imps']
                revenue = publisher['stats']['revenue']

                if imps == None:
                    imps = 0

                if revenue == None:
                    revenue = 0

                sql = """INSERT INTO appnexus_basic_stats (publisher_id, publisher_name, impressions, revenue) VALUES ({0}, {1}, {2}, {3})""".format(id, json.dumps(name), imps, revenue)
                self.cursor.execute(sql)

        self.cursor.execute('commit')


if __name__ == "__main__":
    appNexusBasicStats = AppNexusBasicStats()
    appNexusBasicStats.createReportChunkUrls()
    appNexusBasicStats.connectToDB()
    appNexusBasicStats.createTable()
    appNexusBasicStats.makeCallsAndWriteToDB()

