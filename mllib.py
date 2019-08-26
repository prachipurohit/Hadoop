#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init("/spark")
import pyspark


# In[2]:


from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression


# In[3]:


spark=SparkSession.builder.appName('MLLib').getOrCreate()


# In[4]:


df=spark.read.csv("/root/Desktop/pollution - Copy.csv",header=True,inferSchema=True)


# In[5]:


df.printSchema()


# In[6]:


df.columns


# In[7]:


from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


# In[8]:


assembler = VectorAssembler(inputCols = ['SO2', 'NO2', 'RPM', 'SPM'] , outputCol = "features")


# In[9]:


output = assembler.transform(df)


# In[10]:


output.printSchema()


# In[16]:


final = output.select("features","Asthma")


# In[17]:


final.show()


# In[18]:


train,test = final.randomSplit([0.7,0.3])


# In[19]:


test.describe().show()


# In[20]:


train.describe().show()


# In[21]:


from pyspark.ml.regression import LinearRegression
lr = LinearRegression(labelCol= 'Asthma')


# In[22]:


lrModel = lr.fit(train)


# In[23]:


print("Coefficients : {} , Intercepts : {}".format(lrModel.coefficients,lrModel.intercept))


# In[24]:


test_results = lrModel.evaluate(test)


# In[25]:


test_results.residuals.show()


# In[26]:


test_results.r2


# In[27]:


test_results.rootMeanSquaredError


# In[28]:


test_results.meanSquaredError


# In[29]:


from pyspark.sql.functions import corr
df.select(corr('Asthma','SO2')).show()



# In[ ]:




