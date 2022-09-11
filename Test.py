#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName('spark_hdfs_to_hdfs').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")


# In[14]:


MASTER_NODE_INSTANCE_NAME="cluster-a1c7-m"


# In[15]:


log_files_rdd = sc.textFile('hdfs://{}/data/logs_example/*'.format(MASTER_NODE_INSTANCE_NAME))


# In[16]:


splitted_rdd = log_files_rdd.map(lambda x: x.split(" "))
selected_col_rdd = splitted_rdd.map(lambda x: (x[0], x[3], x[5], x[6]))


# In[18]:


columns = ["ip","date","method","url"]
logs_df = selected_col_rdd.toDF(columns)
logs_df.createOrReplaceTempView('logs_df')
print(type(logs_df),dir(logs_df))


# In[25]:


sql = f"""
  SELECT
  url,
  count(*) as count
  FROM logs_df
  WHERE url LIKE '%/article%'
  GROUP BY url
  """
article_count_df = spark.sql(sql)
print(" ### Get only articles and blogs records ### ")
article_count_df.show(5)


# In[27]:


# In[30]:


article_count_df.write.save('hdfs://{}/data/article_count_df2'.format(MASTER_NODE_INSTANCE_NAME), format='csv', mode='overwrite')

