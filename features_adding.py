#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd


# In[11]:


def add_taxi_Ndays_rolling(df, days):
    """
    This function calculates and adds additional columns for rolling average taxi_in/taxi_out time per airport per day.
    
    Args:
        df - df to process as DataFrame
        days - Days to calculate rolling number for taxi_in, taxi_out time
    Output:
        processed DataFrame is returned back
    """
    cols={'origin':['origin_airport_id', 'taxi_out'],
             'destination':['dest_airport_id','taxi_in']}
    
    df = df.sort_values(['fl_date']) #Sorting by fl_date just in case it was not sorted before. 
                                        #It is important for rolling average
    
    #Iterating the keys in cols which has columns we interested in.
    for key in cols.keys():
        
        #First we calculate average taxi time per airport per day
        df_taxi=df[[cols[key][0], 'fl_date',  cols[key][1]]].groupby([cols[key][0], 'fl_date']).mean().reset_index()

        #Based on our average taxi time we can calculate rolling average
        df_taxi_roll=df_taxi.groupby([cols[key][0]]).rolling(days, on='fl_date'
                                                                           ).agg({cols[key][1]:'mean'}).reset_index()
        #Renaming column to avoid collision during merging
        df_taxi_roll.rename(columns={cols[key][1]: str(days) +'d ' + cols[key][1]}, inplace=True)
        
        #Merging with initial DataFrame
        df=df.merge(df_taxi_roll, on=[cols[key][0], 'fl_date' ] , how='left')
    return df


# In[12]:


def add_traffic_rolling(df, days):
    """
    This function calculates and adds additional column for rolling average number of flights per airport per day.
    
    Args:
        df - DataFrame to process.
        days - Days as integer to calculate rolling average
        
    Output:
        dataframe - initial dataframe with additional column
    """
    cols = ['origin_airport_id', 'dest_airport_id']
    
    df = df.sort_values(['fl_date']) #Sorting by fl_date just in case it was not sorted before. 
                                        #It is important for rolling average
    for item in cols:
        #Now calculating trafic per airport per day. Also will calculate N - days rolling average.
        count_flight=df[[item, 'fl_date', 'mkt_carrier']].groupby([item, 'fl_date'
                                                                             ]).count().reset_index()
        
        count_flights_roll= count_flight.groupby([item]).rolling(days, on='fl_date'
                                                                       ).agg({'mkt_carrier':'mean'}).reset_index()
        #Renaming to avoid collision during merging
        count_flights_roll.rename(columns={'mkt_carrier': str(days) + 'd roll flts ' + item}, inplace=True)
        
        #Merging
        df=df.merge(count_flights_roll, on=[item, 'fl_date' ] , how='left')
        
    return df

