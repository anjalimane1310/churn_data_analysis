#!/usr/bin/env python
# coding: utf-8

# # Customer Churn Risk Analysis – SQL Insights (PySpark)
# 
# In this notebook, we analyze customer churn risk data using Spark SQL.  
# This dataset was prepared from raw telecom data using PySpark ETL in a previous notebook.
# 

# In[2]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ChurnAnalysisSQL').getOrCreate()


# ## Step 1: Load Final Transformed Data
# 
# We load the final churn-labeled dataset (`churn_final.csv`) for SQL-based analysis.
# 

# In[10]:


churn_df = spark.read.csv(r'D:\Pyspark\portfolio_projects\churn_data_analysis\churn_final.csv', header = True)


# In[11]:


churn_df.show(5)


# ## Step 2: Register DataFrame as Temporary SQL View
# 
# We register the DataFrame as a SQL temp view to run SQL queries directly on it.
# 

# In[12]:


churn_df.createOrReplaceTempView('churn_data')


# ## Step 3: Business Questions & SQL Queries
# 
# We answer the following questions:
# 
# 1. What is the distribution of churn risk?
# 2. Does age affect churn risk?
# 3. Which cities have the highest churn?
# 4. How does average usage vary by churn level?
# 5. Which plan types are more prone to churn?
# 

# In[13]:


# churn risk distribution

spark.sql(""" 
        SELECT churn_risk, COUNT(*) as total_customers
        from churn_data
        group by churn_risk
        order by total_customers desc
""").show()


# In[17]:


# avg age by churn risk
spark.sql("""
        SELECT churn_risk, ROUND(avg(age),1) as avg_age, COUNT(*) as customer_count
        from churn_data
        group by churn_risk
        order by churn_risk
""").show()


# In[18]:


# cities with highest high-risk customers
spark.sql("""
        SELECT city, COUNT(*) as high_risk_customers
        from churn_data
        where churn_risk = 'High'
        group by city
        order by high_risk_customers desc
""").show()


# In[21]:


# usage behaviour by churn risk

spark.sql("""
        SELECT churn_risk,
        ROUND(AVG(avg_calls),2) as avg_calls,
        ROUND(AVG(avg_data_used),2) as avg_data_used,
        ROUND(AVG(num_complaints),2) as avg_complaints
        from churn_data
        group by churn_risk
""").show()


# In[22]:


# plan type vs churn risks

spark.sql("""
        SELECT plan_type, churn_risk, COUNT(*) as count
        from churn_data
        group by plan_type, churn_risk
        order by plan_type, churn_risk
""").show()

