# AppNexus API Test
Code to test AppNexus' API for this StackOverflow question: http://stackoverflow.com/questions/30673678/dimensions-not-returned-by-appnexus-api

To make test API calls:

Clone the git repository, edit `appnexus-default.properties` with a valid set of API credentials, make `appnexusTestReport.py` executable (i.e. `chmod +x appnexusTestReport.py`) and run it:

Currently, the dimensions are missing from the response:

    $ ./appnexusTestReport.py 
    imps_total,imps_resold,publisher_filled_revenue
    70348087,44791802,1066.929657
