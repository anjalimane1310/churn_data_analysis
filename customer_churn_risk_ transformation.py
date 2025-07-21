#!/usr/bin/env python
# coding: utf-8

# # Customer Churn Risk Analysis using PySpark
# 
# This project processes telecom customer data to analyze churn risk based on behavior like call activity, data usage, and complaints.
# 
# **Stack Used**: PySpark, SQL, Pandas  
# **Data Source**: Simulated (based on telecom churn patterns)
# 
# The final output is a per-customer churn risk classification (Low / Medium / High) based on:
# - Average calls 
# - Number of complaints
# - Average data usage
# 

# ## Step 1: Import Libraries and Initialize Spark
# 

# In[1]:


import pyspark
from pyspark.sql import SparkSession #starting a pyspark session
from pyspark.sql.types import *
from pyspark.sql.functions import *


# In[2]:


spark = SparkSession.builder.appName('Pysparkchurndata').getOrCreate()


# In[3]:


spark


# ## Step 2: Load Datasets
# 
# We are loading three CSV files:
# - `customers.csv`
# - `usage.csv` (monthly call/data logs)
# - `complaints.csv` (customer support issues)
# 

# In[4]:


#to read customers.csv file and to read first row as column name
df_customers = spark.read.csv(r'path\customers_large.csv', header = True)


# In[5]:


#to read usage.csv file and to read first row as column name
df_usage = spark.read.option('header','true').csv(r'path\usage_large.csv')


# In[6]:


#to read complaints.csv file and to read first row as column name
df_complaints = spark.read.option('header','true').csv(r'path\complaints_large.csv')


# ## Step 3: Data Cleaning
# 
# This includes:
# - Type casting `date` and `string` fields
# - Checking nulls or bad records
# 

# In[8]:


# to check null values.
def null_count(df):
    return df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])


null_count(df_customers).show()
null_count(df_usage).show()
null_count(df_complaints).show()


# In[10]:


# casting date column for df_customers
df_customers = df_customers.withColumn('join_date', to_date('join_date','yyyy-MM-dd'))


# In[17]:


# casting columns for df_usage
df_usage = df_usage.withColumn('date',df_usage['date'].cast(DateType())).withColumn('calls_made',
                                  df_usage['calls_made'].cast(IntegerType())).withColumn('data_used_gb',
                                                                                         df_usage['data_used_gb'].cast(FloatType()))


# In[19]:


# castn+ing date column for df_complaints
df_complaints = df_complaints.withColumn('complaint_date',df_complaints['complaint_date'].cast(DateType()))


# ## Step 4: Join Datasets
# 
# We join all three files to get one master usage+complaint dataset per customer/month.
# 

# In[20]:


master_df = df_customers.join(df_usage, on = 'customer_id', how = 'inner').join(df_complaints, on = 'customer_id', how = 'left')


# In[21]:


master_df.show()


# ## Step 5: Aggregation
# 
# We calculate the following per customer:
# - `avg_calls`
# - `avg_data_used_gb`
# - `num_complaints`
# 

# In[23]:


# aggregating data in one dataframe

agg_df = master_df.groupBy('customer_id').agg(
    avg('calls_made').alias('avg_calls'),
    avg('data_used_gb').alias('avg_data_used'),
    count('complaint_type').alias('num_complaints')
)


# In[24]:


agg_df.show()


# ## Step 6: Churn Risk Tagging
# 
# Churn risk is categorized based on thresholds:
# 
# - **High**: very low avg_calls and high complaints
# - **Medium**: moderate usage with some complaints or low data usage
# - **Low**: everything else
# 

# In[32]:


# calculating churn risk
agg_df = agg_df.withColumn("churn_risk", when( (col('avg_calls') < 10) & (col('num_complaints') >= 2), 'High')
                               .when( (col('avg_calls') < 15) & (col('num_complaints') >= 1) , 'Medium')
                               .when( col('avg_data_used') < 1.0, 'Medium')
                               .otherwise('Low')
                              )


# ## Step 7: Final Dataset
# 
# We join churn metrics with customer profile fields (age, city, plan_type, etc.)
# This gives us a clean, one-row-per-customer `churn_final` DataFrame.
# 

# In[33]:


churn_final = df_customers.join(agg_df, on ='customer_id', how='left')


# In[34]:


churn_final = churn_final.withColumn('avg_data_used',round(col('avg_data_used'),2)).withColumn('avg_calls',round(col('avg_calls'),2))


# In[28]:


churn_final.printSchema()


# ## Step 8: Save Output for Next Notebook
# 
# This dataset will be used in a second notebook for SQL-based churn insights.
# 

# In[36]:


churn_final.toPandas().to_csv("path\churn_final.csv", index=False)

