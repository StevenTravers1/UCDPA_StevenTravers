# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 20:09:16 2022

@author: STEVEN.TRAVERS
"""


import pandas as pd
#import numpy as np
import time
import datetime
from bs4 import BeautifulSoup, NavigableString
import requests
from selenium import webdriver
from  random import randrange

st = time.time()


#Create a function to filter fuel price dataset
def filter_oilprice_dataset(dataset, column_list, fuel_type , country_list):
    
    selected_countries = country_list
    column_filter = column_list
    data = dataset
    
    filtered_data = data[(data["Country Name"].isin(selected_countries)) & (data["Product Name"]==fuel_type)]
    filtered_data = filtered_data[column_filter]
    return(filtered_data)

def fuel_list_generator():
    #specify the url that we are trying to scrape
    link = 'https://ec.europa.eu/energy/observatory/reports/'
    r = requests.get(link)
    html_doc = r.content
    soup = BeautifulSoup(html_doc,'html.parser')
    print(soup.prettify())
    all_refs = []
    
    #Search the html for any links and add them to the all_refs list
    for d in soup.findAll('a',href=True):
        all_refs.append(d['href'])
    
    #convert to dataFrame and rename column
    df = pd.DataFrame(all_refs)
    df.columns= ['link']
    
    #Filter out all links that contain "Raw_Data" and also only contain 2019 / 2022 as these 
    #are the only years being used
    contains = df[df['link'].str.contains("raw_data")]
    
    
    contains_selected_years = contains[contains['link'].str.startswith(("2019","2022"))].reset_index(drop=True)
    
    #Define a list that i wil use to hold the final link values needed to read excels from source
    link_list = []
    
    #The like in which we read the data from has the same string text as far as "reports/" so 
    # I have now looped through the list of links we created to come up with a final
    #list of links.
    for i in range(len(contains_selected_years)):
        link_list.append('http://ec.europa.eu/energy/observatory/reports/'+str(contains_selected_years.loc[i][0]))
    return(link_list)




def scrape_traffic_count(link):
    
    browser = webdriver.Edge('C:/Users/steven.travers/Desktop/UCD Data Course/msedgedriver.exe')
    year=link[-20:-16]
    browser.get(link)
    time.sleep(randrange(5,7))
    html_doc = browser.page_source
    soup = BeautifulSoup(html_doc,'html.parser')
    tbody =soup.tbody
    trs = tbody.contents
    dates_row = trs[2]
    dates_list = list()
    
    for td in dates_row.contents[2:-6] :
        if isinstance(td, NavigableString):
            continue
        dates_list.append(td.string)
    
    scraped_dates = pd.DataFrame(dates_list)
    scraped_dates.columns = ["Dates"]
    
    tc_row = trs[4]
    tc_list = list()
    for td in tc_row.contents[2:-6] :
        if isinstance(td, NavigableString):
            continue
        tc_list.append(td.string)
    
    scraped_tc = pd.DataFrame(tc_list)
    scraped_tc.columns = ["Traffic Count"]
    
    joined_data = scraped_dates.join(scraped_tc)
    joined_data["Year"]=year
    browser.quit()
    return(joined_data)


def get_last_day_of_month(day):
    next_month = day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)


def link_generator(end_dates,start_dates):
    link_list = []
    
    for i in range(0,24):
        try:
            temp_link = "https://trafficdata.tii.ie/tfmonthreport.asp?sgid=XzOA8m4lr27P0HaO3_srSB&spid=130DE8EB2080&reportdate="+str(start_dates[i])+"&enddate="+str(end_dates[i])+"&intval=11"
            link_list.append(temp_link)
        except:   
            return(link_list)



#define dataframe
excel_data = pd.DataFrame()
link_list = fuel_list_generator()
#Loop through all links and use Pandas to read in the raw data and append to bottom.
for link in link_list:
    new_data = pd.read_excel(link)
    excel_data = excel_data.append(new_data)
    #print(link)

#filter data for only the relevent countries and fuel types. 

keep_columns = ["Prices in force on","Country Name","Product Name","Prices Unit","Weekly price with taxes"]
fuel_type = "Euro-super 95"
country_list =  ["Austria","Germany","Ireland","Italy","Spain","Portugal"]

filtered_excel_data = filter_oilprice_dataset(dataset = excel_data, column_list = keep_columns, fuel_type = fuel_type, country_list = country_list).sort_values(['Country Name', "Prices in force on"])

#remove time element on prices in force on to just show yyyy-mm-dd
filtered_excel_data['Prices in force on'] = pd.to_datetime(filtered_excel_data['Prices in force on']).dt.date
#formate date to be dd-mm-yyyy


#create two dataframes with list of all weeks beginning on monday for 2019 & 2022
week_2019 = pd.date_range(start='2019-01-07',end = "2019-12-31", freq='W-MON')
#get last day of previous month   - had 2022-12-31 here but returned nan for any dte after this week.
prev_month = datetime.date.today().replace(day=1)-datetime.timedelta(days=1)
week_2022 = pd.date_range(start='2022-01-03',end = prev_month, freq='W-MON')
week_2019_df = pd.DataFrame(week_2019)
week_2022_df = pd.DataFrame(week_2022)
all_weekly_dates = week_2019_df.append(week_2022_df)
all_weekly_dates.columns =["Prices in force on"]
all_weekly_dates['Prices in force on'] = pd.to_datetime(all_weekly_dates['Prices in force on']).dt.date

del(week_2019,week_2022,week_2019_df,week_2022_df)

joined_data = pd.merge(all_weekly_dates,filtered_excel_data,on = 'Prices in force on',how="outer")

time_after_fuelprice = time.time()

currentmonth = datetime.datetime.now().month
start_dates = []
end_dates = []


for month in range(1, 13):
    #print(get_last_day_of_month(datetime.date(2019, month, 1)))
    end_dates.append(get_last_day_of_month(datetime.date(2019, month, 1)))
    start_dates.append(datetime.date(2019,month,1))

for month in range(1, currentmonth):
    #print(get_last_day_of_month(datetime.date(2022, month, 1)))
    end_dates.append(get_last_day_of_month(datetime.date(2022, month, 1)))
    start_dates.append(datetime.date(2022,month,1))


list_of_links = link_generator(end_dates = end_dates, start_dates = start_dates)

df = pd.DataFrame()

for link in list_of_links:
    current_data = scrape_traffic_count(link)
    df_data_temp = pd.DataFrame(current_data)
    df = df.append(df_data_temp)
    df["date"] = df[["Dates","Year"]].apply(" ".join,axis=1)
    #df['Date']=pd.to_datetime(df['date'],format ='%d %b %Y' ) new
    df['date']=pd.to_datetime(df['date'],format ='%d %b %Y' )
df_temp_ste  = df
df = df.drop(columns =["Dates","Year"])
df = df.reindex(columns=["date","Traffic Count"])
df.columns = ["Date","Traffic Count"]
df = df.reset_index(drop=True)
print(df)


et = time.time()

elapsed_time_1 = time_after_fuelprice-st
elapsed_time_2 = et-time_after_fuelprice
elapsed_time_3 = et-st

print("Time to import fuel price data:",elapsed_time_1,"seconds")
print("Time to import traffic count data:",elapsed_time_2,"seconds")
print("Time to import all data:",elapsed_time_3,"seconds")

# Export CSV's
#df.to_csv('C:/Users/steven.travers/Desktop/UCD Data Course/Traffic_Count.csv')
#joined_data.to_csv('C:/Users/steven.travers/Desktop/UCD Data Course/Fuel_price.csv')
