#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import requests
import pandas as pd


# In[ ]:


app_key='some key'


# In[ ]:


def req_result(url, params):
    params["key"]=app_key
    params["aggregateHours"]=24
    params['unitGroup']='metric'
    params['contentType']='json'
    params['dayStartTime']='0:0:00'
    params['dayEndTime']='0:0:00'
    
    
    res=requests.get(url, params)
    return res.json()


# In[ ]:


url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'


# In[ ]:


df = pd.read_csv('cities_and_dates.csv', sep=',', index_col=0, parse_dates=[1])


# In[ ]:


df['wspd']=""


# In[ ]:


df['visibility']=""


# In[ ]:


df['weathertype']=""


# In[ ]:


df['conditions']=""


# In[ ]:


df.dtypes


# In[ ]:


df.head()


# In[ ]:


get_ipython().run_cell_magic('time', '', "i=0\nfor index, row in df.iterrows():\n    dict_weather =req_result(url, {'startDateTime':row['fl_date'].strftime('%Y-%m-%d') + 'T00:00:00',\n                     'endDateTime':row['fl_date'].strftime('%Y-%m-%d') + 'T23:59:00',\n                    'location':row['city_name']})\n    try:\n        loc_value = list(dict_weather['locations'].keys())[0]\n\n\n        dict_weather['locations'][loc_value]['values'][0]['wspd']\n        df.at[index, 'wspd']=float(dict_weather['locations'][loc_value]['values'][0]['wspd'])\n        df.at[index, 'visibility']=float(dict_weather['locations'][loc_value]['values'][0]['visibility'])\n        df.at[index, 'weathertype']=dict_weather['locations'][loc_value]['values'][0]['weathertype']\n        df.at[index, 'conditions']=dict_weather['locations'][loc_value]['values'][0]['conditions']\n    except TypeError:\n        pass")


# In[ ]:


df.to_csv('cities_and_dates_weather.csv')


# In[ ]:


print("Saved ")

