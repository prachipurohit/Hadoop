#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
import matplotlib.pyplot as pl


# In[3]:


import findspark
findspark.init("/spark")


# In[5]:


from pyspark import SparkContext


# In[6]:


df= pd.read_csv("/root/Desktop/pollution - Copy.csv")


# In[7]:


df.columns


# In[8]:


df.head(20)


# In[26]:


pl.figure(figsize=(15,10))
pl.xticks(rotation=90)
pl.xlabel("Cities")
pl.ylabel("RPM Conc in ug/m3")
pl.bar(df['Cities'],df['RPM'],color = ['orange'])


# In[27]:


pl.figure(figsize=(15,10))
pl.xticks(rotation=90)
pl.xlabel("Cities")
pl.ylabel("SPM Conc in ug/m3")
pl.bar(df['Cities'],df['SPM'],color = ['red'])


# In[28]:


pl.figure(figsize=(15,10))
pl.xticks(rotation=90)
pl.xlabel("Cities")
pl.ylabel("SO2 Conc in ug/m3")
pl.bar(df['Cities'],df['SO2'],color = ['blue'])


# In[30]:


pl.figure(figsize=(15,10))
pl.xticks(rotation=90)
pl.xlabel("Cities")
pl.ylabel("NO2 Conc in ug/m3")
pl.bar(df['Cities'],df['NO2'],color = ['green'])


# In[ ]:




