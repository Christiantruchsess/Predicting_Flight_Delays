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
        s
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


# In[1]:


def replace_nan_with_mean(df,column):
    """
    This function replaces all NaN values for a given column in a dataframe with the mean of the column values,
        _without_ taking outliers into consideration when calculating said mean.
    Args:
        df - Pandas DataFrame 
        column - column of DataFrame, input as a string. 
    Output:
        Processed DataFrame is returned.
    """
    # The .describe() method for Pandas DataFrames outputs a Pandas Series; index number 4 corresponds to 
    # Quartile 1, index number 6 to Quartile 3. The Inter-Quartile Range (IQR) is then calculated as Q3 - Q1.
    Q1 = df[column].describe()[4]
    Q3 = df[column].describe()[6]
    IQR = float(Q3 - Q1)
    # An outlier threshold is calculated as 1.5 times the IQR. Any value that is below Q1 minus the threshold,
    # or above Q3 plus the threshold, is considered an outlier. 
    outlier_threshold = 1.5 * IQR
    lower_limit = Q1 - outlier_threshold
    upper_limit = Q3 + outlier_threshold
    
    # Use boolean operators to define subset of column values that exclude outliers
    subset_without_outliers = df[(df[column] > lower_limit) & (df[column] < upper_limit)][column]
    
    # Calculate the mean for said subset. 
    mean_without_outliers = subset_without_outliers.mean()
    
    # Replace NaN values with previously calculated mean, using .fillna() Pandas method.
    df[column].fillna(mean_without_outliers,inplace=True)
    
    # Return processed DataFrame
    return df


# In[2]:


def make_dates_ordinal(df, dates_column):
    """
    This function converts the dates of a DataFrame column to integers, in order to easily fit the data to a regression model. 
    
    More specifically, the function toordinal() returns the proleptic Gregorian ordinal of a date.
    
    In simple terms datetime.toordinal() returns the day count from the date 01/01/01
    
    Though Gregorian calendar was not followed before October 1582, several computer
        systems follow the Gregorian calendar for the dates that comes even before October 1582.
        Python's date class also does the same.
    
    Args:
        df - Pandas DataFrame 
        dates_column - column of DataFrame, input as a string. All values in column must be 
            of type datetime64[ns].
    Output:
        Processed DataFrame is returned.
    """
    
    # The function imports the required datetime module.
    import datetime as dt
    
    # Applies datetime.toordinal() function to desired column of DataFrame.
    df[dates_column] = df[dates_column].map(dt.datetime.toordinal)
    
    # Returns processed DataFrame
    return df


# In[ ]:


def distill_features(df, full_features = ['fl_date','mkt_unique_carrier','branded_code_share','mkt_carrier','mkt_carrier_fl_num',
                                          'op_unique_carrier','tail_num','op_carrier_fl_num','origin_airport_id','origin',
                                          'origin_city_name','dest_airport_id','dest','dest_city_name','crs_dep_time','crs_arr_time',
                                          'dup','crs_elapsed_time','flights','distance'],
                        removal_list=['mkt_unique_carrier','branded_code_share','mkt_carrier','op_unique_carrier','op_carrier_fl_num',
                                        'tail_num','origin','origin_city_name','dest','dest_city_name','dup','flights'],
                         target='arr_delay'):
    """
    This function reduces the features of a DataFrame to only those relevant to the analysis or model trained at hand, including
    the target variable. 
    
    Args:
        df - Pandas DataFrame 
        full_features - List of features to be reduced. Defaults to case-specific "flights_test" features if no input is given.
        removal_list - List of features to be removed. Defaults to case-specific "flights_test" features if no input is given.
        target - Target variable to be included in final DataFrame. Defaults to case-specific 'arr_delay' if no input is given. 
    Output:
        Processed DataFrame with reduced number of features is returned.
    """
    
    # Implement list comprehension to build `training_features` list composed.
    training_features = [ x for x in full_features if x not in removal_list]
    
    # Append target variable column to DataFrame
    training_features.append('arr_delay')
    
    # Redefine and return DataFrame
    df = df[training_features]
    return df


# In[ ]:


def make_month_dummies(df, date_column):
    df['month']=df[date_column].dt.month
    df = pd.get_dummies(df, columns=['month'])
    return df

