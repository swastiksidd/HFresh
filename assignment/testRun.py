"""

Created By : Siddharth Sethia
Date : 27/10/2021
Description : This code is built to test our application with sample data.

"""


# import sys
# import os
import pytest
from bins.src.core import recipe
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import bins.utils.logging_session as logging_session
import bins.utils.error_log as error_log
import bins.config.config as config

def test_preprocess_json():
	raw_recipe = spark.read.format("json").load(test_path)
	recipe(test_path,dest_path,execution_date).preprocess_json(spark) #This will store the output at dest_path 
	
	expected_data = [
						("Roasted Beef Tenderloin","beef","PT25M","PT25M",1500,1500,3000),
						("Tiramisu","Tiri","PT4H","PT1H",14400,3600,18000),
						("Hot Roast Beef Sandwiches","beef","PT20M","PT20M",1200,1200,2400)
					]
	expected_schema = StructType([
									StructField("name",StringType(),True),
									StructField("ingredients",StringType(),True),
									StructField("cookTime",StringType(),True),
									StructField("prepTime",StringType(),True),
									StructField("cookTimeCov",IntegerType(),True),
									StructField("prepTimeCov",IntegerType(),True),
									StructField("total_cooking_time",IntegerType(),True)								
								])
	
	expected_df = spark.createDataFrame(expected_data,expected_schema)
	expected = expected_df.orderBy('name').collect()
	
	actual_df = spark.read.format("parquet").load(dest_path+"/date_of_execution={}".format(execution_date))
	actual = actual_df.orderBy('name').collect()
	
	assert actual == expected, logger.info("Testing of pre-process data Failed")
	logger.info("Testing of pre-process data Passed")
	

def test_calc_avg_time():
	recipe(test_path,dest_path,execution_date).calc_avg_time(spark)
	
	expected_schema = StructType([
									StructField("difficulty",StringType(),True),
									StructField("avg_total_cooking_time",StringType(),True)								
								])
	
	expected_data = [("medium","45.0")]
	
	expected_df = spark.createDataFrame(expected_data,expected_schema)
	expected = expected_df.orderBy("difficulty").collect()
	
	actual_df = spark.read.csv(dest_path+"/report.csv/*",header="true")
	actual = actual_df.orderBy("difficulty").collect()
	
	assert actual == expected, logger.info("Testing of avg time calculation Failed")
	logger.info("Testing of avg time calculation Passed")

if __name__ == "__main__":
	
	logger = logging_session.getloggingSession()
	logger.info("creating spark instance")
	
	# creating spark instance.
	spark = SparkSession.builder.appName("Test_run").getOrCreate()

	# Test Parameters
	test_path = config.test_path
	dest_path = config.dest_path
	execution_date = config.test_date
	
	try:
		logger.info("started testing preprocessing data")
		test_preprocess_json()
		logger.info("started testing calculate avg time duration")
		test_calc_avg_time()
	
	except Exception as e:
		logger.info("Testing Job Failed.")
		error_log.get_errorlog()
