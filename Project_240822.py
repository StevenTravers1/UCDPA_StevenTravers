# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 10:02:58 2022

@author: STEVEN.TRAVERS
"""

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

#Added a line of code to see how certain steps take
st = time.time()


#Create a function to filter fuel price dataset
def filter_oilprice_dataset(dataset, column_list, fuel_type , country_list):
    
    selected_countries = country_list
    column_filter = column_list
    data = dataset
    
    filtered_data = data[(data["Country Name"].isin(selected_countries)) & (data["Product Name"]==fuel_type)]
    filtered_data = filtered_data[column_filter]
    return(filtered_data)

# Function to generate a list of all links needed to read fuel price excels
def fuel_list_generator():
    #specify the url that we are trying to scrape
    link = 'https://ec.europa.eu/energy/observatory/reports/'
    r = requests.get(link)
    html_doc = r.content
    soup = BeautifulSoup(html_doc,'html.parser')
    print(soup.prettify())
    all_refs = []
    
    #Search the html for any hrefs and add them to the all_refs list
    for d in soup.findAll('a',href=True):
        all_refs.append(d['href'])
    
    #convert to dataFrame and rename column
    df = pd.DataFrame(all_refs)
    df.columns= ['link']
    
    #Filter out all links that contain "Raw_Data" and also only contain 2018 / 2022 as these 
    #are the only years being used
    df_raw = df[df['link'].str.contains("raw_data")]
    
    
    df_raw_years = df_raw[df_raw['link'].str.startswith(("2018","2022"))].reset_index(drop=True)
    
    #Define a list that i wil use to hold the final link values needed to read excels from source
    link_list = []
    
    #The link in which we read the data from has the same string text as far as "reports/" so 
    # I have now looped through the list of links we created to come up with a final
    #list of links.
    for i in range(len(df_raw_years)):
        link_list.append('http://ec.europa.eu/energy/observatory/reports/'+str(df_raw_years.loc[i][0]))
    
    #return the list of links to excel files, ready to be read in.
    return(link_list)



# Function to scrape the data from traffic count website. 
def scrape_traffic_count(link):
    # using selenium to open edge and go to the correct link. 
    browser = webdriver.Edge('C:/Users/steven.travers/Desktop/UCD Data Course/msedgedriver.exe')
    #took the year value from link for future use.
    year=link[-20:-16]
    browser.get(link)
    # Needed to give selenium time to load webpage. Trial and error found 5ish seconds was ok
    time.sleep(randrange(5,7))
    html_doc = browser.page_source
    soup = BeautifulSoup(html_doc,'html.parser')
    tbody =soup.tbody
    trs = tbody.contents
    dates_row = trs[2]
    dates_list = list()
    
    #searching the dates row and extracting dates, [2:-6] used to avoid reading in the time value(not needed) and the average monthly values.
    for td in dates_row.contents[2:-6] :
        if isinstance(td, NavigableString):
            continue
        dates_list.append(td.string)
    
    scraped_dates = pd.DataFrame(dates_list)
    scraped_dates.columns = ["Dates"]
    
    # traffic count was in row 4 of table, similar method to above to extract information
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
    #close browser
    browser.quit()
    
    # returned joined dataframe containing dates, traffic count and the year value. 
    return(joined_data)

# Function to get the last day of the month, needed for end dates to scrape traffic data.
def get_last_day_of_month(day):
    next_month = day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)

# Function to generate a list of links by passing the start and end dates.
def link_generator(end_dates,start_dates):
    link_list = []
    
    for i in range(0,24):
        try:
            temp_link = "https://trafficdata.tii.ie/tfmonthreport.asp?sgid=XzOA8m4lr27P0HaO3_srSB&spid=130DE8EB2080&reportdate="+str(start_dates[i])+"&enddate="+str(end_dates[i])+"&intval=11"
            link_list.append(temp_link)
        except:   
            return(link_list)



#define empty dataframe
fuel_data = pd.DataFrame()

#create a list of links for the fuel data excels from earlier function. 
link_list = fuel_list_generator()
#Loop through all links and use Pandas to read in the raw data and append to bottom.
for link in link_list:
    new_data = pd.read_excel(link)
    fuel_data = fuel_data.append(new_data)
    

#create lists to filter data for only the relevent countries and fuel types. 
keep_columns = ["Prices in force on","Country Name","Product Name","Prices Unit","Weekly price with taxes"]
fuel_type = "Euro-super 95"
country_list =  ["Austria","Germany","Ireland","Italy","Spain","Portugal"]

#Filter the fuelprice dataset using above 3 filters. Only Ireland needed for project but all other countries will be used in real life work.
filtered_fuel_data = filter_oilprice_dataset(dataset = fuel_data, column_list = keep_columns, fuel_type = fuel_type, country_list = country_list).sort_values(['Country Name', "Prices in force on"])

#remove time element on prices in force on to just show yyyy-mm-dd and convert prices to numeric values
filtered_fuel_data['Prices in force on'] = pd.to_datetime(filtered_fuel_data['Prices in force on']).dt.date
filtered_fuel_data['Weekly price with taxes']=filtered_fuel_data['Weekly price with taxes'].astype("string")
filtered_fuel_data['Weekly price with taxes'] = pd.to_numeric(filtered_fuel_data["Weekly price with taxes"].str.replace(",","", regex=True))


#create two dataframes with list of all weeks beginning on 1st Monday for 2018 & 2022
weeks_2018 = pd.date_range(start='2018-01-01',end = "2018-12-31", freq='W-MON')
#get last day of previous month   - had 2022-12-31 here but returned nan for any dte after this week.
prev_month = datetime.date.today().replace(day=1)-datetime.timedelta(days=1)
weeks_2022 = pd.date_range(start='2022-01-03',end = prev_month, freq='W-MON')
weeks_2018 = pd.DataFrame(weeks_2018)
weeks_2022 = pd.DataFrame(weeks_2022)
all_weekly_dates = weeks_2018.append(weeks_2022)
all_weekly_dates.columns =["Prices in force on"]
all_weekly_dates['Prices in force on'] = pd.to_datetime(all_weekly_dates['Prices in force on']).dt.date

del(weeks_2018,weeks_2022)

# Merging all the weekly dates with fuel dataframe to ensure all weeks in fuel dataframe had values. 
joined_fuel_data = pd.merge(all_weekly_dates,filtered_fuel_data,on = 'Prices in force on',how="outer")

#time to see how long it took to read in and sort fuel price dataframe
time_after_fuelprice = time.time()

# Current month used as a stopper i.e. cant get traffic count for future months
currentmonth = datetime.datetime.now().month

#Needed start and end dates of each month as they are the only vairables used in the traffic count website link
start_dates = []
end_dates = []

#code to get start and end dates for 2018 and 2022
for month in range(1, 13):
    #print(get_last_day_of_month(datetime.date(2018, month, 1)))
    end_dates.append(get_last_day_of_month(datetime.date(2018, month, 1)))
    start_dates.append(datetime.date(2018,month,1))

for month in range(1, currentmonth):
    #print(get_last_day_of_month(datetime.date(2022, month, 1)))
    end_dates.append(get_last_day_of_month(datetime.date(2022, month, 1)))
    start_dates.append(datetime.date(2022,month,1))

# Using link generator function to generate a list of links
list_of_links = link_generator(end_dates = end_dates, start_dates = start_dates)

#initialising dataframe for traffic count data
df_2018 = pd.DataFrame()

for link in list_of_links[0:12]:
    current_data = scrape_traffic_count(link)
    df_data_temp = pd.DataFrame(current_data)
    df_2018 = df_2018.append(df_data_temp)
    #dates in format "01 Jan" and year "2018" changed this to " DD MM YYYY"
    df_2018["date"] = df_2018[["Dates","Year"]].apply(" ".join,axis=1)
    df_2018['date']=pd.to_datetime(df_2018['date'],format ='%d %b %Y' )

df_2022 = pd.DataFrame()

for link in list_of_links[12:]:
    current_data = scrape_traffic_count(link)
    df_data_temp = pd.DataFrame(current_data)
    df_2022 = df_2022.append(df_data_temp)
    #dates in format "01 Jan" and year "2018" changed this to " DD MM YYYY"
    df_2022["date"] = df_2022[["Dates","Year"]].apply(" ".join,axis=1)
    df_2022['date']=pd.to_datetime(df_2022['date'],format ='%d %b %Y' )



df = df_2018.append(df_2022)

#Re-organising dataframe
df = df.drop(columns =["Dates","Year"])
df = df.reindex(columns=["date","Traffic Count"])
df.columns = ["Date","Traffic Count"]
df = df.reset_index(drop=True)
print(df)

#End time of code
et = time.time()


# Export CSV's used during project to save me webscraping every time
#df.to_csv('C:/Users/steven.travers/Desktop/UCD Data Course/22082022/Traffic_Count.csv')
#joined_data.to_csv('C:/Users/steven.travers/Desktop/UCD Data Course/22082022/Fuel_price.csv')

#Show how many Na's are in each column
print(df.isna().sum())
print(joined_fuel_data.isna().sum())


#Fill in Na values. 

fuel_prices = joined_fuel_data
traffic_data = df.copy() # Created a copy of DF so i could work on cleaning data and could go back when mistakes were made
list_countries = pd.DataFrame(fuel_prices['Country Name'].drop_duplicates())




def fill_missing(dataset, country_name):
    # Filtered data by countryname and isnull to show where missing values were. 
    filtered_data = dataset[(dataset['Country Name']== country_name)  | (dataset['Country Name'].isnull())].reset_index(drop='True')
    bfillcols = ["Country Name", "Product Name","Prices Unit"]
    #For constant values, ie. country name and fuel type, i used bfiil.
    filtered_data.loc[:,bfillcols] = filtered_data.loc[:,bfillcols].bfill()    
    intercols = ["Weekly price with taxes"]
    #For non-constant missing values, i.e. the fuel price, i interpolated the data using linear method
    filtered_data[intercols] = filtered_data[intercols].interpolate(method='linear')    
    #returned the data with no missing values
    return(filtered_data)

#austria_data = fill_missing(fuel_prices,country_name = "Austria")
#germany_data = fill_missing(fuel_prices,country_name = "Germany")
ireland_data = fill_missing(fuel_prices,country_name = "Ireland")
#italy_data = fill_missing(fuel_prices,country_name = "Italy")
#portugal_data = fill_missing(fuel_prices,country_name = "Portugal")
#spain_data = fill_missing(fuel_prices,country_name = "Spain")

# added different variables such as week number and formatted columns.
traffic_data['week_number'] = traffic_data['Date'].dt.week
traffic_data['Traffic Count']=pd.to_numeric(traffic_data['Traffic Count'])
traffic_data['Date']= pd.to_datetime(traffic_data['Date']) 
Weekly_traffic_data = traffic_data.groupby(['week_number',pd.Grouper(key='Date',freq='W')])['Traffic Count'].mean().reset_index().sort_values('Date')

Weekly_traffic_data['Date']=pd.to_datetime(Weekly_traffic_data['Date']) - pd.to_timedelta(6, unit='d')
Weekly_traffic_data['Year'] = Weekly_traffic_data['Date'].dt.year
Weekly_traffic_data = Weekly_traffic_data.loc[Weekly_traffic_data['Year'].isin([2018,2022])]
Weekly_traffic_data['Date']=pd.to_datetime(Weekly_traffic_data['Date']).dt.date
#Weekly_traffic_data = Weekly_traffic_data.drop(columns=["Year","week_number"]).reset_index(drop='True')
# print(Weekly_traffic_data)  #just used to make sure i had what i wanted

#Merge the fuel price data and the traffic data using effectively the "date" column from each dataframe.
all_ireland_data = pd.merge(ireland_data,Weekly_traffic_data,how='inner',right_on = 'Date',left_on = 'Prices in force on')


#Code to generate plots - May be a more efficient way of generating plots to be considered.
# Pulling only 2018 and 2022 data respectively into dataframes
line_18 = all_ireland_data[all_ireland_data['Year'] == 2018]
line_22 = all_ireland_data[all_ireland_data['Year'] == 2022]
line_18 =line_18 .iloc[:-1,:]

#Creating a copy of the dataframe for similar reasons to above, just to work on code and be able to go back when mistakes made
test_data = line_18.copy()

#Cleaning data and renaming columns
test_data=test_data.drop(columns=["Prices in force on","Country Name","Product Name","Date","Year","Prices Unit"])
test_data.columns= ["Pump price 2018","week","traffic count 2018"]
test_data_22= line_22.copy().reset_index(drop='True')
test_data_22=test_data_22.drop(columns=["Prices in force on","Country Name","Product Name","Date","Year","Prices Unit"])
test_data_22.columns= ["Pump price 2022","week","traffic count 2022"]

#Merging 2018 and 2022 data on week number and keeping year to be able to seperate.
graph_data = pd.merge(test_data,test_data_22,on="week",how="outer")
#graph_data = graph_data.fillna(0)

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

#May be more efficient way of doing this!
#Changing x axis on plot to show month names instead of week numbers, went thruogh dataFrame to figure
# out where each month started in terms of week numbers but now on graph month names are on week 1 of month, might
# be worth changing to the middle of month

month_starts = [1,6,10,14,19,23,27,32,36,40,45,49]
month_names = ['Jan','Feb','Mar','Apr','May','Jun',
               'Jul','Aug','Sep','Oct','Nov','Dec'] 





#Code to create plot number 1

fig, ax = plt.subplots(2,1,sharey=True)
# Plot the CO2 levels time-series in blue
plot_timeseries(ax[0], graph_data['week'], graph_data['Pump price 2018']/1000, 'blue', "Months", "Pump price € / L")
#Code gotten online to create second y axis on each plot as twinx wouldnt work otherwise.
ax2 = np.array([a.twinx() for a in ax.ravel()]).reshape(ax.shape)
plot_timeseries(ax2[0], graph_data['week'], graph_data['traffic count 2018']/1000, 'red', "Months", "Traffic count (1,000s)")


# Plot the 2022 data 
plot_timeseries(ax[1], graph_data['week'], graph_data['Pump price 2022']/1000, 'blue', "Months", "Pump price € / L")
plot_timeseries(ax2[1], graph_data['week'], graph_data['traffic count 2022']/1000, 'red', "Months", "Traffic count (1,000s)")

ax[0].set_ylim([1,2.3])
ax[1].set_ylim([1,2.3])

ax2[0].set_ylim([0,200])
ax2[1].set_ylim([0,200])

ax[0].set_xticks(month_starts)
ax[1].set_xticks(month_starts)

ax[0].set_xticklabels(month_names)
ax[1].set_xticklabels(month_names)


ax2[0].set_xticks(month_starts)
ax2[1].set_xticks(month_starts)

ax2[0].set_xticklabels(month_names)
ax2[1].set_xticklabels(month_names)



plt.show()



#Plotting 2018 and 2022 traffic count on graph 2.
fig ,ax3 = plt.subplots()
ax3.plot(graph_data['week'],graph_data['traffic count 2018'])
ax3.set_xlabel('Week number')
ax3.plot(graph_data['week'],graph_data['traffic count 2022'])
ax3.set_xticks(month_starts)
ax3.set_xticklabels(month_names)
plt.show()



final_time = time.time()

#values to print out how long code took to get to fuel price data, traffic data and finish. 
elapsed_time_1 = time_after_fuelprice-st
elapsed_time_2 = et-time_after_fuelprice
elapsed_time_3 = et-st
elapsed_time_4 = final_time-st

#Print time values to screen
print("Time to import fuel price data:",time.strftime("%H:%M:%S", time.gmtime(elapsed_time_1)),"seconds")
print("Time to import traffic count data:",time.strftime("%H:%M:%S", time.gmtime(elapsed_time_2)),"seconds")
print("Time to import all data:",time.strftime("%H:%M:%S", time.gmtime(elapsed_time_3)),"seconds")
print("Time to Run all Code:",time.strftime("%H:%M:%S", time.gmtime(elapsed_time_4)),"seconds")




#


