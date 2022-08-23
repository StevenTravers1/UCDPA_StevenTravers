# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 20:09:16 2022

@author: STEVEN.TRAVERS
"""


import pandas as pd
import numpy as np
import time
import datetime
from bs4 import BeautifulSoup, NavigableString
import requests
from selenium import webdriver
from  random import randrange
import matplotlib.pyplot as plt
import matplotlib
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
    
    #Filter out all links that contain "Raw_Data" and also only contain 2018 / 2022 as these 
    #are the only years being used
    contains = df[df['link'].str.contains("raw_data")]
    
    
    contains_selected_years = contains[contains['link'].str.startswith(("2018","2022"))].reset_index(drop=True)
    
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

#remove time element on prices in force on to just show yyyy-mm-dd and convert prices to numeric values
filtered_excel_data['Prices in force on'] = pd.to_datetime(filtered_excel_data['Prices in force on']).dt.date
filtered_excel_data['Weekly price with taxes']=filtered_excel_data['Weekly price with taxes'].astype("string")
filtered_excel_data['Weekly price with taxes'] = pd.to_numeric(filtered_excel_data["Weekly price with taxes"].str.replace(",","", regex=True))

#formate date to be dd-mm-yyyy

#create two dataframes with list of all weeks beginning on monday for 2018 & 2022
week_2018 = pd.date_range(start='2018-01-01',end = "2018-12-31", freq='W-MON')
#get last day of previous month   - had 2022-12-31 here but returned nan for any dte after this week.
prev_month = datetime.date.today().replace(day=1)-datetime.timedelta(days=1)
week_2022 = pd.date_range(start='2022-01-03',end = prev_month, freq='W-MON')
week_2018_df = pd.DataFrame(week_2018)
week_2022_df = pd.DataFrame(week_2022)
all_weekly_dates = week_2018_df.append(week_2022_df)
all_weekly_dates.columns =["Prices in force on"]
all_weekly_dates['Prices in force on'] = pd.to_datetime(all_weekly_dates['Prices in force on']).dt.date

del(week_2018,week_2022,week_2018_df,week_2022_df)

joined_data = pd.merge(all_weekly_dates,filtered_excel_data,on = 'Prices in force on',how="outer")

time_after_fuelprice = time.time()

currentmonth = datetime.datetime.now().month
start_dates = []
end_dates = []






for month in range(1, 13):
    #print(get_last_day_of_month(datetime.date(2018, month, 1)))
    end_dates.append(get_last_day_of_month(datetime.date(2018, month, 1)))
    start_dates.append(datetime.date(2018,month,1))

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
#df.to_csv('C:/Users/steven.travers/Desktop/UCD Data Course/22082022/Traffic_Count.csv')
#joined_data.to_csv('C:/Users/steven.travers/Desktop/UCD Data Course/22082022/Fuel_price.csv')

#Show how many Na's are in each column
print(df.isna().sum())
print(joined_data.isna().sum())


#Fill in Na values. 

fuel_prices = joined_data
traffic_data = df.copy()


list_countries = pd.DataFrame(fuel_prices['Country Name'].drop_duplicates())



filtered_data = fuel_prices[(fuel_prices['Country Name']== "Ireland")  | (fuel_prices['Country Name'].isnull())].reset_index(drop='True')
bfillcols = ["Country Name", "Product Name","Prices Unit"]
filtered_data.loc[:,bfillcols] = filtered_data.loc[:,bfillcols].bfill()    
intercols = ["Weekly price with taxes"]
print(filtered_data["Weekly price with taxes"].dtype)



def fill_missing(dataset, country_name):
    filtered_data = dataset[(dataset['Country Name']== country_name)  | (dataset['Country Name'].isnull())].reset_index(drop='True')
    bfillcols = ["Country Name", "Product Name","Prices Unit"]
    filtered_data.loc[:,bfillcols] = filtered_data.loc[:,bfillcols].bfill()    
    intercols = ["Weekly price with taxes"]
    filtered_data[intercols] = filtered_data[intercols].interpolate(method='linear')    
    return(filtered_data)

#austria_data = fill_missing(fuel_prices,country_name = "Austria")
#germany_data = fill_missing(fuel_prices,country_name = "Germany")
ireland_data = fill_missing(fuel_prices,country_name = "Ireland")
#italy_data = fill_missing(fuel_prices,country_name = "Italy")
#portugal_data = fill_missing(fuel_prices,country_name = "Portugal")
#spain_data = fill_missing(fuel_prices,country_name = "Spain")

#ireland_data = pd.merge(ireland_data,traffic_data,left_on='Prices in force on',right_on='Date',how='left')

print(df)

def custom_mean(df):
    return df.mean(skipna=True)

traffic_data['week_number'] = traffic_data['Date'].dt.week
traffic_data['Traffic Count']=pd.to_numeric(traffic_data['Traffic Count'])
traffic_data['Date']= pd.to_datetime(traffic_data['Date']) 
Weekly_traffic_data = traffic_data.groupby(['week_number',pd.Grouper(key='Date',freq='W')])['Traffic Count'].mean().reset_index().sort_values('Date')

Weekly_traffic_data['Date']=pd.to_datetime(Weekly_traffic_data['Date']) - pd.to_timedelta(6, unit='d')
Weekly_traffic_data['Year'] = Weekly_traffic_data['Date'].dt.year
Weekly_traffic_data = Weekly_traffic_data.loc[Weekly_traffic_data['Year'].isin([2018,2022])]
Weekly_traffic_data['Date']=pd.to_datetime(Weekly_traffic_data['Date']).dt.date
Weekly_traffic_data = Weekly_traffic_data.drop(columns=["Year","week_number"]).reset_index(drop='True')
print(Weekly_traffic_data)
# print(Weekly_traffic_data)


all_ireland_data = pd.merge(ireland_data,Weekly_traffic_data,how='inner',right_on = 'Date',left_on = 'Prices in force on')

line_18 = plot_data[plot_data['Year'] == 2018]
line_22 = plot_data[plot_data['Year'] == 2022]
line_18 =line_18 .iloc[:-1,:]


test_data = line_18.copy()


test_data=test_data.drop(columns=["Country Name","Product Name","Date","Year"])


test_data.columns= ["Pump price 2018","traffic count 2018","Month","week","month_abb"]

test_data_22= line_22.copy().reset_index(drop='True')
test_data_22=test_data_22.drop(columns=["Country Name","Product Name","Date","Year","Month","month_abb"])
test_data_22.columns= ["Pump price 2022","traffic count 2022","week"]

graph_data = pd.merge(test_data,test_data_22,on="week",how="outer")

# Define a function called plot_timeseries
def plot_timeseries(axes, x, y, color, xlabel, ylabel):
  # Plot the inputs x,y in the provided color
  axes.plot(x, y, color=color)
  # Set the x-axis label
  axes.set_xlabel(xlabel)
  # Set the y-axis label
  axes.set_ylabel(ylabel, color=color)
  # Set the colors tick params for y-axis
  axes.tick_params('y', colors=color)


month_starts = [1,6,10,14,19,23,27,32,36,40,45,49]
month_names = ['Jan','Feb','Mar','Apr','May','Jun',
               'Jul','Aug','Sep','Oct','Nov','Dec'] 





fig, ax = plt.subplots(2,1,sharey=True)
# Plot the CO2 levels time-series in blue
plot_timeseries(ax[0], graph_data['week'], graph_data['Pump price 2018']/1000, 'blue', "Time (Months)", "Pump price € / L")
ax2 = np.array([a.twinx() for a in ax.ravel()]).reshape(ax.shape)
plot_timeseries(ax2[0], graph_data['week'], graph_data['traffic count 2018']/1000, 'red', "Time (years)", "Traffic count (1,000s)")


# Plot the CO2 levels time-series in blue
plot_timeseries(ax[1], graph_data['week'], graph_data['Pump price 2022']/1000, 'blue', "Time (Months)", "Pump price € / L")
plot_timeseries(ax2[1], graph_data['week'], graph_data['traffic count 2022']/1000, 'red', "Time (Months)", "Traffic count (1,000s)")

ax.set_ylim([1,2.1])
ax2[0].set_ylim([60000,160000])
ax2[1].set_ylim([60000,160000])

ax.set_xticks(month_starts)
ax.set_xticklabels(month_names)

plt.show()




fig ,ax3 = plt.subplots()
ax3.plot(graph_data['week'],graph_data['traffic count 2018'])
ax3.set_xlabel('Week number')
ax3.plot(graph_data['week'],graph_data['traffic count 2022'])
ax3.set_xticks(month_starts)
ax3.set_xticklabels(month_names)
plt.show()













