import pandas as pd
from pandas import json_normalize
import json
import urllib.request
import numpy as np 
import datetime
d = datetime.datetime.today()

#First load in JSON data for a range of time, set the index to 'ProgramId' and make the datetime format consistent with the API call
"""
with urllib.request.urlopen(http://programs.dsireusa.org/api/v1/getprogramsbydate/[startdate]/[enddate]/json) as url:
        data = json.loads(url.read().decode())
#Loads the JSON data into a Pandas dataframe and updates formatting
df = json_normalize(data["data"])
df['LastUpdate'] = pd.to_datetime(df['LastUpdate'], format = "%m/%d/%Y")
df = df.set_index('ProgramId')
"""
#After loading JSON data from the API the first time, the file can be saved locally and updates will take less time

#This code formats the dataframe for the API calls
df = pd.read_csv("PythonStuff/DSIRE_Program_Data.csv")
df = df.set_index('ProgramId')
df['LastUpdate'] = pd.to_datetime(df['LastUpdate'])
print(df.columns)
print(df[['State','CategoryName','LastUpdate','TypeName','Budget','StartDate','Technologies']])

#Function to retrieve the current date in the API call format
def current_date():
    year = str(d.year)
    if d.month < 10:
        month = '0'+str(d.month)
    else:
        month = d.month
    #day is set to today's date minus one day because sometimes today's actual date has connection issues with the server
    if d.day-1 < 10:
        day = '0'+str(d.day-1)
    else:
        day = d.day-1
    date = str(year)+str(month)+str(day)
    return date

#Function to retrieve the last updated date of the dataframe in the API call format
def last_updated_date():
    updated_date = df['LastUpdate'].max()
    updated_date = updated_date.strftime('%Y%m%d')
    return updated_date

#Function that accesses the DSIRE API in the correct format by pulling data from the last time the local dataframe was updated through yesterday's date (date functions defined above).
def add_to_df(df):
    #Function that updates the local copy of the dataframe (specified in the argument 'dataframe') with the most recent update form the DSIRE API
    #Creates the http link to the API based on the last time the local dataset was updated
    API_link = str("http://programs.dsireusa.org/api/v1/getprogramsbydate/")+str(last_updated_date())+str("/")+str(current_date())+str("/json")
    with urllib.request.urlopen(API_link) as url:
        API_data = json.loads(url.read().decode())
    #Loads the JSON data into a Pandas dataframe and updates formatting
    df_update = json_normalize(API_data["data"])
    df_update['LastUpdate'] = pd.to_datetime(df_update['LastUpdate'], format = "%m/%d/%Y")
    df_update = df_update.set_index('ProgramId')
    #Updates any existing programs in the local file with new data
    df.update(df_update)
    #Add new data not currently in the local file
    new_data = df_update.loc[~df_update.index.isin(list(df.index))]
    df = df.append(new_data)
    return df

#Call the add_to_df function on the dataframe ('df')
Up_to_date_df = add_to_df(df)

df['StartDate'] = pd.to_datetime(df['StartDate'])

def update_date(row):
    if row['StartDate'] is pd.NaT:
        return row['LastUpdate']
    else:
        return row['StartDate']

df['RecentDate'] = df.apply(lambda row: update_date(row), axis=1)

print(df[['RecentDate', 'StartDate', 'LastUpdate']])

#Update the local copy of the dataset with information from the last update by writing the updated dataframe to .csv
#add_to_df(df).to_csv("Name_of_File.csv")