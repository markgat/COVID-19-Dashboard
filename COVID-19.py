#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Previously used os to save results on set path
# import os
# from IPython.display import Image
# Include path for title image for notebook
# Path ="Images/"
# Image(filename= Path + "covid19.jpg", width=970, height=900)
# Photo by Martin Sanchez on Unsplash


# In[2]:


# Packages and librairies to import
import numpy as np
import pandas as pd
# import pyodbc
from datetime import datetime
from google.cloud import bigquery

# Set print options of numpy float kind as default float handle of .format() string method
# Formatter takes dictionary as an argument
np.set_printoptions(formatter={"float_kind":"{:f}".format})


# In[3]:


# Created easy reference list to data files in John Hopkins repository
# and preemptively creat placeholders for datafames within dictionary named df_list
df_names = ["confirmed_global", "deaths_global", "recovered_global"]
df_list = [None for df in df_names]
df_dict = dict(zip(df_names, df_list))

# *** Comment blocked previous code in case url reference of data does not work and must go local 

# Load raw data into notebook as dataframe objects
# raw_data_confirmed = pd.read_csv("/Users/ /Desktop/COVID-19-master(30:07:21)/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv")
# raw_data_deaths = pd.read_csv("/Users/ /Desktop/COVID-19-master(30:07:21)/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv")
# raw_data_recovered = pd.read_csv("/Users/ /Desktop/COVID-19-master(30:07:21)/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv")

# ***

# Url string template to reference individual file locations in remote repository
url_part = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_"

# For looped to retrieve confirmed, recovered, and deaths CSVs to convert into dataframes and fill df_dict accordingly
for key, value in df_dict.items():
    value = pd.read_csv(url_part+key+".csv")
    
    df_dict[key] = value
    # Print head of dataframes and print confirmation messages to test process
    print(value.head())
    print(key+" cases datafame created\n--------------------------------------------------------")
    
# Reassign dataframes to variables and delete dictionary

raw_data_confirmed = df_dict["confirmed_global"]
raw_data_deaths = df_dict["deaths_global"]
raw_data_recovered = df_dict["recovered_global"]

del df_dict

# Examining the data resources, note that rows for globally recovered patients are less than the other dataframes
print("Shape of global confirmed data: ", raw_data_confirmed.shape)
print("Shape of global deaths data: ", raw_data_deaths.shape)
print("Shape of global recovered data: ", raw_data_recovered.shape)


# In[4]:


# Unpivoting data in order to simplify value aggregations
raw_data_confirmed2 = pd.melt(raw_data_confirmed, id_vars=["Province/State", "Country/Region", "Lat", "Long"], var_name=["Date"])
raw_data_deaths2 = pd.melt(raw_data_deaths, id_vars=["Province/State", "Country/Region", "Lat", "Long"], var_name=["Date"])
raw_data_recovered2 = pd.melt(raw_data_recovered, id_vars=["Province/State", "Country/Region", "Lat", "Long"], var_name=["Date"])

print(raw_data_confirmed2.head())
print(raw_data_deaths2.head())
print(raw_data_recovered2.head())


# In[5]:


# This section may cause errors !!!
# Reformatting date values may be necessary!!!
raw_data_confirmed2["Date"] = pd.to_datetime(raw_data_confirmed2["Date"])
raw_data_deaths2["Date"] = pd.to_datetime(raw_data_deaths2["Date"])
raw_data_recovered2["Date"] = pd.to_datetime(raw_data_recovered2["Date"])

print("Shape of new confirmed dataframe: ", raw_data_confirmed2.shape)
print("Shape of new deaths dataframe: ", raw_data_deaths2.shape)
print("Shape of new recovered dataframe: ", raw_data_recovered2.shape)


# In[6]:


# From 4/4/20 number of rows have multiplied by 74 from testing dataset 
# Renaming ambiguous value columns
raw_data_confirmed2.columns = raw_data_confirmed2.columns.str.replace("value", "Confirmed")
raw_data_deaths2.columns = raw_data_deaths2.columns.str.replace("value", "Deaths")
raw_data_recovered2.columns = raw_data_recovered2.columns.str.replace("value" ,"Recovered")


# In[7]:


# Looking to see if there are any null values
print(' Confirmed \n', raw_data_confirmed2.isnull().sum())
print("-----")
print(' Deaths \n', raw_data_deaths2.isnull().sum())
print("-----")
print(' Recovered \n', raw_data_recovered2.isnull().sum())


# In[8]:


# From 4/4/20, there wass 74 more null values within Recovered, meaning theres was 1 extra row containing Null
# values before melt/unpivoting the data

# Fill in empty cells in 'Province/State' column with value in 'Country/Region'
raw_data_confirmed2["Province/State"].fillna(raw_data_confirmed2["Country/Region"], inplace = True)
raw_data_deaths2["Province/State"].fillna(raw_data_deaths2["Country/Region"], inplace = True)
raw_data_recovered2["Province/State"].fillna(raw_data_recovered2["Country/Region"], inplace = True)


# In[9]:


# Check for any null values one last time
print(' Confirmed \n',raw_data_confirmed2.isnull().sum())
print("-----")
print(' Deaths \n',raw_data_deaths2.isnull().sum())
print("-----")
print(' Recovered \n', raw_data_recovered2.isnull().sum())


# In[10]:


# Round Lat and Long values to 3 decimal places to avoid entry discrepencies when joining
# 4 locations share this problem when using 4/4/20 dataset

raw_data_confirmed2["Lat"].update(raw_data_confirmed2["Lat"].round(3).astype(np.float64))
raw_data_deaths2["Lat"].update(raw_data_deaths2["Lat"].round(3).astype(np.float64))
raw_data_recovered2["Lat"].update(raw_data_recovered2["Lat"].round(3).astype(np.float64))

raw_data_confirmed2["Long"].update(raw_data_confirmed2["Long"].round(3).astype(np.float64))
raw_data_deaths2["Long"].update(raw_data_deaths2["Long"].round(3).astype(np.float64))
raw_data_recovered2["Long"].update(raw_data_recovered2["Long"].round(3).astype(np.float64))

# Now perform full joins of all dataframes

# Confirmed dataframe with Deaths dataframe first
full_join = raw_data_confirmed2.merge(raw_data_deaths2[["Province/State","Country/Region","Lat", "Long","Date",
                                                        "Deaths"]],
                                     how = "outer",
                                     on = ["Province/State","Country/Region","Lat","Long","Date"])

print("Shape of first join:", full_join.shape)

# Finish with outer join to Recovered dataframe

# Since cell block initially defines full_join dataframe by merge of "confirm" and "deaths", there is no concern
# in ruining full_join dataframe by merging full_join dataframe to "recovered" repeatedly when cell block runs more
# than once

full_join = full_join.merge(raw_data_recovered2[["Province/State", "Country/Region", "Lat", "Long","Date",
                                                 "Recovered"]],
                           how = "outer",
                           on = ["Province/State", "Country/Region", "Lat", "Long", "Date"])

print("Shape of second join:", full_join.shape)

full_join.head()
# Note that the rows have increased and that the original recovered dataframe's shape was less than confirmed and
# deaths 


# In[11]:


# Check if there are any null values in the new dataframe (especially long and lat for future geoplotting)

full_join.isnull().sum()


# In[12]:


# From the John Hopkins data from up 'til April 4th, 2020 there were 74 entries that were null in lat, long, confirmed,
# and deaths after joins and melt

# pd.melt() added 74 additional rows per original row

# Row 38 ("", Canada) within the original Recovered dataframe DNE within the original Confirmed and Deaths dataframes 
# causing the 74 nulls in confirmed, and deaths columns as mentioned above

# Additionally, there were 14 less rows for the original Recovered dataframe as to original Confirmed and Deaths.
# Including the row mentioned above, this caused 15 rows from the original Confirmed and Deaths to not be 
# matched/joined causing 1110 null values within Recovered column after melt.
# Note: Recovered after melt and join contained 1110 null values and 1110/74 = 15

testdf = full_join[["Province/State", "Country/Region","Lat","Long","Date","Confirmed","Recovered","Deaths"]]
# isnull() returns table-like array of bool where it is True if value in cell is Null/NaN/empty, and .any(axis=1)
# checks rows for any True/non-zero values from the bool array while reducing columns to create Series of bool 
nulldf = testdf[testdf.isnull().any(axis=1)]
# Nulldf is dataframe of all rows that have null values
print(nulldf.shape)
nulldf.head()


# In[13]:


# Add new column of Month and Year using date-to-str formatting and str-to-date parsing to create new column of date datatypes 
full_join["Month-Year"] = pd.Series([datetime.strptime(x,"%b-%Y") for x in full_join["Date"].dt.strftime('%b-%Y').tolist()]) 

print(full_join.head())


# In[14]:


# Test to filter data of Anhui by days
# New df with Anhui data
test = full_join[full_join['Province/State'] == 'Anhui']

# Create copy of the same data
full_join2 = test.copy()
full_join2.head()


# In[15]:


# new date - 1 columns
# Dates shifted one day ahead to reference the day before
full_join2['Date - 1'] = full_join2['Date'] + pd.Timedelta(days=1)
full_join2.rename(columns={'Confirmed':'Confirmed - 1', 'Deaths' : 'Deaths - 1', 'Recovered': 'Recovered - 1',
                          'Date' : 'Date Minus 1'}, inplace=True)

#Join the two dataframes along test's Date and full_join2's Date - 1
full_join3 = test.merge(full_join2[['Province/State', 'Country/Region', 'Confirmed - 1', 'Deaths - 1',
                                   'Recovered - 1', 'Date - 1', 'Date Minus 1']], how = 'outer',
                       left_on = ['Province/State', 'Country/Region', 'Date'],
                       right_on = ['Province/State','Country/Region', 'Date - 1'])
# Preview of modified DF with shifted dates
full_join2.head()


# In[16]:


# Preview of merged DF with adjacent date data
full_join3.head()


# In[17]:


# Added daily confirmed cases
full_join3['Confirmed Daily'] = full_join3['Confirmed'] - full_join3['Confirmed - 1']
full_join3.head()


# In[18]:


# Next apply to entire data set

# Reassigning full_join2 to a copy of unaltered fully joined dataframe
full_join2 = full_join.copy()

# Applying the date - 1 column
full_join2['Date - 1'] = full_join2['Date'] + pd.Timedelta(days=1)
full_join2.rename(columns = {'Confirmed': 'Confirmed - 1', 'Deaths': 'Deaths - 1', 'Recovered': 'Recovered - 1',
                          'Date': 'Date Minus 1'}, inplace = True)

# Join the two entire datasets of the original full join and it's - 1 day altered form
full_join3 = full_join.merge(full_join2[['Province/State', 'Country/Region', 'Confirmed - 1', 'Deaths - 1',
                                        'Recovered - 1', 'Date - 1', 'Date Minus 1']], how = 'left',
                            left_on = ['Province/State', 'Country/Region', 'Date'],
                            right_on = ['Province/State', 'Country/Region', 'Date - 1'])

# Checked 500 rows to ensure full_join2's "- 1" column values were were not fully empty/null from final join
full_join3.head(500)


# In[19]:


full_join3.shape


# In[20]:


# Calculations for the daily deltas
full_join3['Confirmed Daily'] = full_join3['Confirmed'] - full_join3['Confirmed - 1']
full_join3['Deaths Daily'] = full_join3['Deaths'] - full_join3['Deaths - 1']
full_join3['Recovered Daily'] = full_join3['Recovered'] - full_join3['Recovered - 1']

full_join3.head()


# In[21]:


# 3 new columns added, with no addition of rows
full_join3.shape


# In[22]:


# Filling the calculations for first day records since there are no previous entries to compare with
full_join3.loc[full_join3['Date'] == '2020-01-22', 'Confirmed Daily'] = full_join3['Confirmed']
full_join3.loc[full_join3['Date'] == '2020-01-22', 'Deaths Daily'] = full_join3['Deaths']
full_join3.loc[full_join3['Date'] == '2020-01-22', 'Recovered Daily'] = full_join3['Recovered']

# Fill all NaN as 0 in order to allow data conversion
full_join3["Confirmed"] = full_join3["Confirmed"].fillna(0)
full_join3["Deaths"] = full_join3["Deaths"].fillna(0)
full_join3["Recovered"] = full_join3["Recovered"].fillna(0)
full_join3["Confirmed Daily"] = full_join3["Confirmed Daily"].fillna(0)
full_join3["Deaths Daily"] = full_join3["Deaths Daily"].fillna(0)
full_join3["Recovered Daily"] = full_join3["Recovered Daily"].fillna(0)

# Converted datatypes to integer 64 for numeric columns
full_join3["Confirmed"].update(full_join3["Confirmed"].astype(np.int64))
full_join3["Deaths"].update(full_join3["Deaths"].astype(np.int64))
full_join3["Recovered"].update(full_join3["Recovered"].astype(np.int64))
full_join3["Confirmed Daily"].update(full_join3["Confirmed Daily"].astype(np.int64))
full_join3["Deaths Daily"].update(full_join3["Deaths Daily"].astype(np.int64))
full_join3["Recovered Daily"].update(full_join3["Recovered Daily"].astype(np.int64))

full_join3.head()

# print column/series datatypes to find datatype mismatch
print(full_join3.dtypes["Province/State"])
print(full_join3.dtypes["Country/Region"])
print(full_join3.dtypes["Lat"])
print(full_join3.dtypes["Long"])
print(full_join3.dtypes["Date"])
print(full_join3.dtypes["Confirmed"])
print(full_join3.dtypes["Deaths"])
print(full_join3.dtypes["Recovered"])
print(full_join3.dtypes["Month-Year"])
print(full_join3.dtypes["Confirmed Daily"])
print(full_join3.dtypes["Deaths Daily"])
print(full_join3.dtypes["Recovered Daily"])

# Rename columns that contain invalid characters when sent to BigQuery

full_join3.rename(columns={"Province/State": "Province_State", "Country/Region": "Country_Region", "Month-Year": "Month_Year",
 "Confirmed Daily": "Confirmed_Daily", "Deaths Daily": "Deaths_Daily", "Recovered Daily": "Recovered_Daily"}, inplace=True)
# In[23]:


# Deleting temporary columns used for daily calculations and comparisons
del full_join3['Confirmed - 1']
del full_join3['Deaths - 1']
del full_join3['Recovered - 1']
del full_join3['Date - 1']
del full_join3['Date Minus 1']

full_join3.head()


# In[24]:


# *** Comment blocked in order to import into SQL SERVER Database instead of file export 

# Export data to csv
# Setting destination path
# path = "/Users/ /Desktop/Learning/Bootcamp Assignments/COVID-19_Dashboard/PowerBI"

# Changing my current work directory
# os.chdir(path)

# full_join3.to_csv('CoronaVirus PowerBI Raw', sep = '\t')

# ***

# Fill NaN cells with 0 so that sql float columns can handle them
# full_join3['Lat'].fillna(0, inplace=True)
# full_join3['Long'].fillna(0, inplace=True)
# full_join3['Confirmed'].fillna(0, inplace=True)
# full_join3['Deaths'].fillna(0, inplace=True)
# full_join3['Recovered'].fillna(0, inplace=True)
# full_join3['Confirmed Daily'].fillna(0, inplace=True)
# full_join3['Deaths Daily'].fillna(0, inplace=True)
# full_join3['Recovered Daily'].fillna(0, inplace=True)



# Create connection to SQL SERVER Database
# conn= pyodbc.connect(
#     "Driver={ODBC Driver 17 for SQL Server};"
#     "Server=;"
#     "Database=;"
#     "UID=;"
#     "PWD=;"
#     "Trusted_Connection=;")

# Create cursor for database
# cursor= conn.cursor()

# # Remove any previous values from existing table in database
# cursor.execute("""TRUNCATE TABLE CoronaVirusPowerBIRaw""")
# conn.commit()


# ***Initially created table

# cursor.execute("""CREATE TABLE CoronaVirusPowerBIRaw(
#                     ID int IDENTITY(0,1) NOT NULL,
#                     Province_State varchar(100),
#                     Country_Region varchar(100),
#                     Lat decimal(6,3),
#                     Long decimal(6,3),
#                     Date date,
#                     Confirmed int,
#                     Deaths int,
#                     Recovered int,
#                     Month_Year varchar(10),
#                     Confirmed_Daily int,
#                     Deaths_Daily int,
#                     Recovered_Daily int,
#                     PRIMARY KEY (ID)
#                 );""")

# conn.commit()
# ***


# Insert values from dataframe
# Accounted for single apostraphes in values causing quotation errors and properly formatted dates 


# for index,row in full_join3.iterrows():
#     cursor.execute(f"""INSERT INTO CoronaVirusPowerBIRaw([Province_State], [Country_Region], [Lat], [Long], [Date],
#                     [Confirmed], [Deaths], [Recovered], [Month_Year], [Confirmed_Daily], [Deaths_Daily],
#                     [Recovered_Daily]) VALUES ('{str(row['Province/State']).replace("'","''")}',
#                     '{str(row['Country/Region']).replace("'","''")}',{row['Lat']},{row['Long']}, CONVERT(date, 
#                     '{row['Date']}', 23), {row['Confirmed']},{row['Deaths']}, {row['Recovered']},'{row['Month-Year']}', 
#                     {row['Confirmed Daily']}, {row['Deaths Daily']},{row['Recovered Daily']});""")
#     conn.commit()


# cursor.close()
# conn.close()


# In[ ]:


# Bigquery code outline

# **Please comment process**
# **Setup conversions for SQL formalities if needed**
# **Setup JSON key**
table_id = ""

client = bigquery.Client()

job_config = bigquery.LoadJobConfig(schema = [bigquery.SchemaField("Province_State", bigquery.enums.SqlTypeNames.STRING),
                                             bigquery.SchemaField("Country_Region", bigquery.enums.SqlTypeNames.STRING),
                                             bigquery.SchemaField("Lat", bigquery.enums.SqlTypeNames.FLOAT),
                                             bigquery.SchemaField("Long", bigquery.enums.SqlTypeNames.FLOAT),
                                             bigquery.SchemaField("Date", bigquery.enums.SqlTypeNames.DATE),
                                             bigquery.SchemaField("Confirmed", bigquery.enums.SqlTypeNames.INTEGER),
                                             bigquery.SchemaField("Deaths", bigquery.enums.SqlTypeNames.INTEGER),
                                             bigquery.SchemaField("Recovered", bigquery.enums.SqlTypeNames.INTEGER),
                                             bigquery.SchemaField("Month_Year", bigquery.enums.SqlTypeNames.DATE),
                                             bigquery.SchemaField("Confirmed_Daily", bigquery.enums.SqlTypeNames.INTEGER),
                                             bigquery.SchemaField("Deaths_Daily", bigquery.enums.SqlTypeNames.INTEGER),
                                             bigquery.SchemaField("Recovered_Daily", bigquery.enums.SqlTypeNames.INTEGER)],
                                    write_disposition = "WRITE_TRUNCATE")

job = client.load_table_from_dataframe(full_join3, table_id, job_config=job_config)

job.result()

