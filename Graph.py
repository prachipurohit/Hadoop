#!/usr/bin/env python
# coding: utf-8

# In[2]:


import findspark
findspark.init("/spark")
import pyspark


# In[3]:


from pyspark.sql import SparkSession


# In[4]:


spark = SparkSession.builder.appName('Graph').getOrCreate()


# In[5]:


df=spark.read.csv("/root/Desktop/pollution.csv",header=True,inferSchema=True)


# In[30]:


df.show()


# In[31]:


df.agg({'NO2':'max'}).show()


# In[33]:


df.agg({'SO2':'max'}).show()


# In[34]:


df.agg({'SPM':'max'}).show()


# In[35]:


df.agg({'RPM':'max'}).show()


# In[21]:


df.orderBy(df['SO2'].desc()).head(1)[0][1]


# In[22]:


df.orderBy(df['NO2'].desc()).head(1)[0][1]


# In[23]:


df.orderBy(df['SPM'].desc()).head(1)[0][1]


# In[24]:


df.orderBy(df['RPM'].desc()).head(1)[0][1]


# In[29]:


df.filter((df['SO2']>40) & (df['NO2']>50) & (df['SPM']>80) & (df['RPM']>100)).select(['State','Cities']).show()


# In[42]:


import matplotlib.pyplot as pl
import pandas as pd
import numpy as np


# In[43]:


x=['Agartala','Aurangabad','Ghaziabad','Delhi']


# In[46]:


y=[50,70,520,466]
series = pd.Series.from_array(y)


# In[50]:


pl.figure(figsize=(12,8))
ax = series.plot(kind='bar')
ax.set_title('Air Concentration')
ax.set_xlabel('Cities')
ax.set_ylabel('Conc in u/gm3')
ax.set_xticklabels(x)
rects = ax.patches
labels = ['SO2','NO2','SPM','RPM']
for rect ,label in zip(rects,labels):
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width()/2 ,height +5 ,label,ha = 'center' , va= 'bottom')


# In[48]:


pl.bar(x,y,color='red')


# In[ ]:




