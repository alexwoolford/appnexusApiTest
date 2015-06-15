#!/usr/bin/env python

import requests
import json
import urllib
import time
import ConfigParser
config = ConfigParser.RawConfigParser()
config.read('appnexus-default.properties')

def testReport():

    s = requests.session()
    credentials = json.dumps({'auth': {'username' : config.get('appnexus', 'username'), 'password' : config.get('appnexus', 'password')}})
    s.post(url = "https://api.appnexus.com/auth", data = credentials)

    reportJson = json.dumps(eval("""\
    {
        "report": {
            "format": "csv",
            "report_interval": "yesterday",
            "row_per": [
                "hour"
            ],
            "columns": [
                "imps_total",
                "imps_resold",
                "publisher_filled_revenue"
            ],
            "report_type": "publisher_analytics"
        }
    }"""))
    response = s.post("http://api.appnexus.com/report?publisher_id={0}".format(config.get('appnexus', 'publisher_id')), data = reportJson)
    print(response.content)
    reportId = json.loads(response.content)['response']['report_id']
    time.sleep(1)
    report = s.get("http://api.appnexus.com/report-download?" + urllib.urlencode({'id': reportId}))
    return report.content

if __name__ == "__main__":
    print(testReport())
