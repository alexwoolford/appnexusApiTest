# AppNexus API Test
Code to test AppNexus' API for this StackOverflow question: http://stackoverflow.com/questions/30673678/dimensions-not-returned-by-appnexus-api

Properties for the Python scripts will be imported from a file called `appnexus-default.properties`. Create a text file that looks like this (but use your own MySQL/AppNexus credentials):

    [appnexus]
    username=alexwoolford
    password=password123
    publisher_id=123456

    [database]
    username=awoolford
    password=password123
    host=localhost
    db=appnexus

First, execute `appnexus_basic_stats.py` to create a list of active publishers, then `appnexus_hourly_stats.py` to get hourly statistics written to the MySQL database.
