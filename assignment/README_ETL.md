# Code Structure

| Folder Name | SubFolder Name | File Name | Description |
| ----------- | -------------- | --------- | ----------- |
| assignment  | bins/config    | config.py | Source Path, Target Path, Execution date (rest are for testing). All the parameters are there in this config. Please make sure to change it before running it in your environment.|
| assignment  | bins/config    | spark_config.py | contains Spark configuration properties. Edit it based on your requirement |
| assignment  | bins/config    | properties.py | contains Log related configuration. Change the log path, file name, file path, log level from here. |
| assignment  | bins/src   | core.py | Contains core logic/methods. You can add up any modules,functions or classes in this directory. |
| assignment  | bins/utils   | logging_session.py | contians logger object. it is used to create and write logs across project. |
| assignment  | bins/utils | error_log.py | contains error formatter. A unique error logging is followed across project. |
| assignment  | bins/utils   | create_spark_inst.py | Used to create spark instance for the project. |
| assignment  | input/   | NA | All the input json files for our process. Change config.py to control the source path. |
| assignment  | output/   | NA | contains output csv file. Change config.py to control target path |
| assignment  | logs/   | *.log | All the logs will be captured in this folder. Control the log path from properties.py |
| assignment  | test_data/   | NA | This contains sample test data to perform testing. |
| assignment  | NA  | driver.py | It is the single point to trigger the project. It contains the main method. |
| assignment  | NA   | executeMe.sh | Contains spark submit command. Trigger the project using "sh executeMe.sh" |
| assignment  | NA   | testRun.py | Contains logic to perform test using test data. |
| assignment  | NA   | executeMe.sh | It is spark submit to trigger testRun.py |

# Approach

- Maintained seperate config files for our application, Logging, and spark configuration.
- Created utilities to log normal logging, error logging and to create spark instance across application.
- Seperated the business logic (core.py) from execution logic (driver.py) using Classes. You can create as many modules, classes in src folder to scale up the project. 
- In terms of processing we are reading json data everyday and after preprocessing, we are writing it on s3/HDFS with date partition .
- As we are maintaing data at daily level (partition), it will give good performance when we read it for the task 2.
- You can run the process for any backdate by changing the execution data value in config file.
- You can change the source and destination path in config file.

# How to Run

- Change config.py, properties.py, spark_config.py based on your requirement. and trigger executeMe.sh using "sh executeMe.sh".
- This code is flexible, because we just need to trigger driver.py and in executeMe.sh we are calculating the location driver.py and triggering it.

# Test cases & Testing

- Using pytest module, we will run unit test cases.
- Is it not installed then install it using pip => pip3 install pytest
- TestData is in test_data folder. These contains 3 records from actual data. Place it in your cluster and then same needs to updated in config.py (test_path).
- The testing process will store the output data (like parquet, report.csv) based on the test data, in dest_location. Config destination location in config.py (dest_path).
- At last we are comparing the actual and expected results and returning the outcome in log file itself.
- To trigger the test process just run testMe.sh using "sh testMe.sh". the logic to test the process is in testRun.py.

## Bonus points

- Config management. => Everything is configurable in the code.
- Data quality checks (like input/output dataset validation). => Taken care in the core.py.
- How would you implement CI/CD for this application? => We can implement it using Jenkins, RLM. Jenkins can be used to build the application. and Then RLM will move the build (package) to target environment. We can also maintain different branches in Git to track any additional changes.
- How would you diagnose and tune the application in case of performance problems? => By looking at the execution plan and DAG, we will identify the stage/task which is taking time. Then We can repartition/Coalesce data. We can use chache/persist on DF. We can remove data skewness using salting technique.
- How would you schedule this pipeline to run periodically? => We can create Autosys Job, which will trigger the execteMe.sh on specified date and time. Also few other scheduler tools like oozie, airflow.
- We appreciate good combination of Software and Data Engineering. => I believe i did justice to this.



Thanks !
