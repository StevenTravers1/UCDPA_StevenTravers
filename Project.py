# -*- coding: utf-8 -*-
"""
#Data comes out on a monday every week
"""
import time
t1 = time.perf_counter()
import numpy as np
import pandas as pd
import datetime


link = 'http://ec.europa.eu/energy/observatory/reports/2022_07_04_raw_data_2106.xlsx'
excel_data = pd.read_excel(link)

selected_countries = ["Austria","Germany","Ireland","Italy","Spain","Portugal"]
selected_columns = ["Prices in force on","Country Name","Product Name","Prices Unit","Weekly price with taxes"]
fuel_type = "Euro-super 95"




start_date = datetime.datetime.strptime("12/01/2015", "%d/%m/%Y")



def time_changer(start_date, day_increment):
    
    temp = datetime.timedelta(days = day_increment) 
    new_date = start_date + temp
    return (new_date)

def filter_oilprice_dataset(dataset, column_list, fuel_type , country_list):
    
    selected_countries = country_list
    column_filter = column_list
    data = dataset
    
    filtered_data = data[(data["Country Name"].isin(selected_countries)) & (data["Product Name"]==fuel_type)]
    filtered_data = filtered_data[column_filter]
    return(filtered_data)
    
 
temp_date = start_date

for i in range (1735,2106):
    print(i)
    result=None
    while result == None:
        try: # lookup continue
            day = temp_date.strftime("%d")
            month = temp_date.strftime("%m")
            year = temp_date.strftime("%Y")
            try:
                link_temp = 'http://ec.europa.eu/energy/observatory/reports/'+str(year)+'_'+str(month)+"_"+str(day)+'_raw_data_'+str(i)+'.xls'
                temp_data = pd.read_excel(link_temp)
            except:
                link_temp = 'http://ec.europa.eu/energy/observatory/reports/'+str(year)+'_'+str(month)+"_"+str(day)+'_raw_data_'+str(i)+'.xlsx'
                temp_data = pd.read_excel(link_temp)
            excel_data = excel_data.append(temp_data)
            #print(link_temp)
                    
        except:
            temp_date = time_changer(temp_date, 7)    
            day = temp_date.strftime("%d")
            month = temp_date.strftime("%m")
            year = temp_date.strftime("%Y")
            try:
                link_temp = 'http://ec.europa.eu/energy/observatory/reports/'+str(year)+'_'+str(month)+"_"+str(day)+'_raw_data_'+str(i)+'.xls'
                temp_data = pd.read_excel(link_temp)
            except:
                link_temp = 'http://ec.europa.eu/energy/observatory/reports/'+str(year)+'_'+str(month)+"_"+str(day)+'_raw_data_'+str(i)+'.xlsx'
                
            
                try:
                    temp_data = pd.read_excel(link_temp)
                    temp_date = time_changer(temp_date, 14)
                    day = temp_date.strftime("%d")
                    month = temp_date.strftime("%m")
                    year = temp_date.strftime("%Y")
                    try:
                        link_temp = 'http://ec.europa.eu/energy/observatory/reports/'+str(year)+'_'+str(month)+"_"+str(day)+'_raw_data_'+str(i)+'.xls'
                        print(link_temp)
                        temp_data = pd.read_excel(link_temp)
                    except:
                        link_temp = 'http://ec.europa.eu/energy/observatory/reports/'+str(year)+'_'+str(month)+"_"+str(day)+'_raw_data_'+str(i)+'.xlsx'
                        temp_data = pd.read_excel(link_temp)
                except:
                     print(link_temp)
                
            excel_data = excel_data.append(temp_data)
        
        temp_date = time_changer(temp_date, 7)
        result=True

#Requests and Beautiful soup 3
# test = pd.read_excel('https://trafficdata.tii.ie/tfmonthreport.asp?sgid=XzOA8m4lr27P0HaO3_srSB&spid=130DE8EB2080&reportdate=2022-07-14&enddate=2022-07-14&dir=-3&excel=1')

#Clean up dataset by filtering columns & rows, then sorting by country - Date

excel_data_filtered = filter_oilprice_dataset(dataset = excel_data, column_list= selected_columns, fuel_type = fuel_type, country_list= selected_countries)
data_sort = excel_data_filtered.sort_values(['Country Name', "Prices in force on"])
data_sort['Prices in force on'] = pd.to_datetime(data_sort['Prices in force on']).dt.date
    
t2 = time.perf_counter()
print((t2-t1)/60)
