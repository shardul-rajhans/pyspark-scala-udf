#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Setting the environment for the PySpark
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["JAVA_HOME"] = "C:\Java\jdk-11.0.13"
os.environ["SPARK_HOME"]= os.getcwd() + "\Lib\site-packages\pyspark"
os.environ["HADOOP_HOME"]= os.getcwd() + "\Lib\site-packages\pyspark"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "\python\lib"
sys.path.insert(0, os.environ["PYLIB"] +"\py4j-0.10.9.5-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"\pyspark.zip")


# In[2]:


from pyspark.sql.functions import lit, col, udf, create_map
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq


# In[3]:


# Creating a Spark Session
spark = SparkSession.builder.appName('PySparkUdfRunner') \
.config('spark.jars', '../scalaudf_2.12-1.0.0.jar')\
.master("local[*]") \
.getOrCreate()


# In[4]:


# Reading Input Dataframe
inputDf = spark.read \
.option("header", value = True) \
.option("delimiter", value = "|") \
.csv(path = "../src/main/resources/input_data.csv")


# In[5]:


# Class used for execute UDF
class UDFExecutor:
    def __init__(self):
        pass
    def apply(self, udf, *args):
        return Column(udf.apply(_to_seq(spark.sparkContext, [arg for arg in args], _to_java_column)))
    
# Accessing UDFBuilder written in Scala
UDFBuilder = spark.sparkContext._jvm.com.example.udf.sdk.UDFBuilder

# UDF for Concatenation of Addressing Fields
addressUDF = UDFBuilder().forAddress()

# UDF for splitting into Addressing Fields
reverseAddressUdf = UDFBuilder().forReverseAddress()

addressUdfResult = inputDf.withColumn("concat_result", 
                                      UDFExecutor().apply(addressUDF, create_map(
                                          lit("_Address_"), col("Address"),
                                          lit("_Country_"), col("Country"),
                                          lit("_PostalCode_"), col("PostalCode")))
                                     )
reverseAddressUdfResult = inputDf.withColumn("split_result", 
                                      UDFExecutor().apply(reverseAddressUdf, col("CompleteAddress"))
                                     )

addressUdfResult.drop("CompleteAddress").show(truncate = False)
reverseAddressUdfResult.drop("Address", "Country", "PostalCode").show(truncate = False)


# In[ ]:





# In[ ]:





# In[ ]:




