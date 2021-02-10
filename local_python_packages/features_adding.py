#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


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
        df_taxi_roll=df_taxi.groupby([cols[key][0]]).rolling(days, on='fl_date', min_periods=2
                                                                           ).agg({cols[key][1]:'mean'}).reset_index()
        #Renaming column to avoid collision during merging
        df_taxi_roll.rename(columns={cols[key][1]: str(days) +'d ' + cols[key][1]}, inplace=True)
        
        #Merging with initial DataFrame
        df=df.merge(df_taxi_roll, on=[cols[key][0], 'fl_date' ] , how='left')
    return df


# In[3]:


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
        
        count_flights_roll= count_flight.groupby([item]).rolling(days, on='fl_date', min_periods=2
                                                                       ).agg({'mkt_carrier':'mean'}).reset_index()
        #Renaming to avoid collision during merging
        count_flights_roll.rename(columns={'mkt_carrier': str(days) + 'd roll flts ' + item}, inplace=True)
        
        #Merging
        df=df.merge(count_flights_roll, on=[item, 'fl_date' ] , how='left')
        
    return df


# In[4]:


def return_outlier_limits(df,column):
    """
    Function calculates Interquartile Range (IQR) in order to return upper and lower limits after which to consider a value an outlier. 
        A limit is defined as 1.5 times the IQR below Quartile 1 (Q1) or above Quartile 3 (Q3).
    
    Args:
        df - Pandas DataFrame 
        column - Column of DataFrame with the aforementioned outliers, input as a string.
    Output:
        List with lower and upper outlier limits.
    """
    
    # The .describe() method for Pandas DataFrames outputs a Pandas Series; index number 4 corresponds to 
    # Quartile 1, index number 6 to Quartile 3. The Inter-Quartile Range (IQR) is then calculated as Q3 - Q1.
    Q1 = df[column].describe()[4]
    Q3 = df[column].describe()[6]
    IQR = float(Q3 - Q1)
    
    # An outlier threshold is calculated as 1.5 times the IQR. 
    outlier_threshold = 1.5 * IQR
    lower_limit = Q1 - outlier_threshold
    upper_limit = Q3 + outlier_threshold
    
    limits = [lower_limit, upper_limit]
   
    return limits


# In[5]:


def remove_outliers(df, column):
    """
    Function removes rows with outliers from a dataframe, as defined by the return_outlier_limits function. 
    
    Args:
        df - Pandas DataFrame 
        column - Column of DataFrame with the aforementioned outliers, input as a string.
    Output:
        Processed DataFrame is returned (subset of original).
    """
   
    # Call return_outlier_limits function to return list `limit` with two values, lower and upper: limit[0] corresponds to the lower limit, 
    # limit[1] to the upper limit. 
    limits = return_outlier_limits(df,column)
    
    # Use boolean operators to define subset of column values that exclude outliers
    df_no_outliers = df[(df[column] > limits[0]) & (df[column] < limits[1])]
    
    return df_no_outliers


# In[6]:


def replace_nan_with_mean(df,column,include_outliers=False):
    """
    This function replaces all NaN values for a given column in a dataframe with the mean of the column values.
   
    Args:
        df - Pandas DataFrame 
        column - Column of DataFrame, input as a string.
        include_outliers - If True, calculates mean of all values,
            if False, does not consider outliers when calculating mean. Defaults to False.
    Output:
        Processed DataFrame is returned.
    """
    if include_outliers == False:
        df_no_outliers = remove_outliers(df,column)
        mean = df_no_outliers[column].mean()
    else:
        mean = df[column].mean()
        
    # Replace NaN values with previously calculated mean, using .fillna() Pandas method.
    df[column].fillna(mean,inplace=True)
   
    # Return processed DataFrame
    return df


# In[7]:


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


# In[8]:


def distill_features(df, desired_features = ['fl_date','mkt_carrier_fl_num','origin_airport_id','dest_airport_id','crs_dep_time',
                                             'crs_arr_time','crs_elapsed_time','distance','arr_delay']):
    df = df[desired_features]
    return df


# In[9]:


def make_month_dummies(df, date_column):
    """
    This function adds dummy variable columns for months.
    
    Args:
        df - Dataframe which needed to be processed.
        date_column as string. Column with dates
    Output:
        Dataframe with dummy varialbles.
    """
    
    df['month']=df[date_column].dt.month
    df = pd.get_dummies(df, columns=['month'])
    return df


# In[10]:


def merging_weather_flights(df_flights, df_weather):
    """
    This function merges weather data information to flights table based on three columns - 
    'fl_date', 'origin_city_name', 'dest_city_name'.
    
    For each row in flights table additional columns will be added (for origin city and dest city) -
        'wspd' - wind speed
        'visibility'  - visibility
        'conditions'  - weather conditions, which is categorical variable and will be converted to the dummy variables.
    Args:
        df_flights - dataframe with flights data.
        df_weather - dataframe with weather data.
    Output:
        df_flights - processed flights dataframe with additional columns.
    """
    
    #Dropping 'weather_type' column.
    try:
        df_weather.drop(columns=['Unnamed: 0','weathertype'], inplace=True)
    except TypeError:
        pass
    
    #Preparing columns for origin city.
    df_weather_origin = df_weather.rename(columns={'city_name':'origin_city_name',
                              'wspd':'origin_city_wspd',
                                'visibility':'origin_visibility',
                                'conditions':'origin_cond'})
    
    #Merging origin city weather to the flights dataframe. Left join is used.
    df_flights = df_flights.merge(df_weather_origin, on=['fl_date', 'origin_city_name'], how='left')
    
    #Preparing columns for dest city.
    df_weather_dest = df_weather.rename(columns={'city_name':'dest_city_name',
                              'wspd':'dest_city_wspd',
                                'visibility':'dest_visibility',
                                'conditions':'dest_cond'})
    
    #Merging destination city weather to the flights dataframe. Left join is used.
    df_flights = df_flights.merge(df_weather_dest, on=['fl_date', 'dest_city_name'], how='left')
    
    #Making dummy variables out of categorical ones:'origin_cond', 'dest_cond'
    df_flights = pd.get_dummies(df_flights, columns=['origin_cond', 'dest_cond'])
    
    #list of different categories. There are some categories which consists of two ones like "Snow, Overcast" 'Rain, Partially cloudy' 
    #The aim is to get rid of them from the dataframe
    list_categories=df_weather['conditions'].unique()
    
    #City prefix
    columns=['origin_cond', 'dest_cond']
    
    for cond in list_categories:
        #Need to find those with comma in the name
        if type(cond) == type('some string') and cond.find(',')>-1:
            #comma found
            lst= cond.split(', ')
            for city_cond in columns:
                if city_cond + '_' + cond in df_flights.columns:
                    df_flights.loc[df_flights[city_cond + '_' + cond]==1, city_cond + '_' + lst[0]]=1 #Updated respective column
                    df_flights.loc[df_flights[city_cond + '_' + cond]==1, city_cond + '_' + lst[1]]=1 #Updated respective column
                    df_flights.drop(columns=[city_cond + '_' + cond], inplace=True) #Dropping the column
    
    #Dropping redundunt columns
    df_flights.drop(columns=['origin_cond_Clear', 'dest_cond_Clear'], inplace=True)
    
    return df_flights


# In[11]:


def quick_split(X,y,train_ratio=0.80):
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X,y,train_size=train_ratio)
    return [X_train,X_test,y_train,y_test]


# In[13]:


def add_dep_delay_Ndays_rolling(df, days):
    """
    This function adds additional column with N-days rolling mean for departure delay per origin airport.
    Args:
        df - Dataframe with flights information
        days - Number of days to calculate rolling mean
    Output:
        df - processed dataframe with additional column
    """
    #Calculating average for aiport per day
    dep_delay_df = df[['fl_date', 'dep_delay','origin_airport_id']].groupby(
                ['origin_airport_id', 'fl_date']).mean().reset_index()
    #Calculate rolling average per airport
    dep_delay_df=dep_delay_df[['fl_date', 'dep_delay','origin_airport_id']].groupby(
            'origin_airport_id').rolling(days, on='fl_date', min_periods=2).agg({'dep_delay':'mean'}).reset_index()
    
    dep_delay_df.rename(columns={'dep_delay':str(days) + ' days roll dep_time'}, inplace=True)
    
    #Merging with initial DataFrame
    df=df.merge(dep_delay_df, on=['origin_airport_id', 'fl_date' ] , how='left')
    return df


# In[1]:


def add_US_holidays(df, df_holidays):
    """
    This function adds additional boolean column to initial dataframe with US national holidays.
    
    Args:
        df - Dataframe with flights information.
        df_holidays - Dataframe with holidays.
    Output:
        df - Processed Dataframe with additional column.
    """
    cols=df_holidays.columns
    df_holidays.rename(columns={cols[0]:'fl_date'}, inplace=True)
    df_holidays.drop(cols[2], inplace=True)
    df = df.merge(df_holidays, on='fl_date', how='left')
    df= pd.get_dummies(df, columns=[cols[1]])
    return df


# In[ ]:




