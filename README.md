# AppNexus API Test
Code to test AppNexus' API for this StackOverflow question: http://stackoverflow.com/questions/30673678/dimensions-not-returned-by-appnexus-api

Properties for the Python scripts will be imported from a file called `appnexus-default.properties`. Create a text file that looks like this (but use your own MySQL/AppNexus credentials):

    [appnexus]
    username=alexwoolford
    password=password123
    publisher_id=123456

    [database]
    username=awoolford
    password=passord123
    host=localhost
    db=appnexus

Currently, the dimensions are missing from the response:

    $ ./appnexusTestReport.py 
    imps_total,imps_resold,publisher_filled_revenue
    70348087,44791802,1066.929657
