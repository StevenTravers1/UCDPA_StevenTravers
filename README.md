# UCDPA_StevenTravers
Work in process:
Repository for the certificate in Data Analytics Project. This projects reads in weekly petrol prices in Ireland along with the m50 traffic count to try to analyse if there is an impact on how much people dirve(traffic count) due to petrol prices. This code needs selenium installed to run. and the path must be updated too.

Total run time of importing fuel excels - 1 min 3 seconds
Total run time of importing Traffic data - 4 min 29 seconds
Total run time of importing all data - 5 min 32 seconds
Total run time ofrunning all code - 5 min 33 seconds


Future updates will include:

Changing traffic count to just cars - to eleminate the impscts of trucks on traffic count.
Storing all data after a full scrape in a csv file to be read in, which will save time reading in traffic data and fuel data from previous years.
Change graphs to monthly data as weekly looks very busy and difficult to look at.
Better use of functions and datetime values. It seems like a lot of times the datetime function was used and could be a little unnecissary. 
Finding sources similar to TII to get multiple EU countries perspectives.
Reading in the other table from TII website describing possible glitches / outliers in data such as storms/ traffic counter being out of action.
Changing the random time when scraping the traffic data - this was used to make sure selenium had the page load in properly. There must be a function in selenium that waits the correct amount of time

There were small problems when trying to run the code on a work laptop - this could be due to vpn issues or problems with TII website. It has ran fine on own pc. 



If there are any updates people would particularly like - reach out to me at steven.travers147@gmail.com

