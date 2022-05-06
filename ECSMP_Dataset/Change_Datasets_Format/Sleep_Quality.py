# module load python/gcc/3.7.9 
# pyspark --deploy-mode client
import pandas as pd
import pyspark
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType
spark = SparkSession.builder.getOrCreate()

df2 = pd.read_excel("/home/ytk244/Final_Project/BD_Original_Datasets/sleep_quality.xlsx", engine="openpyxl")

print(df2.dtypes) # check each column's datatype to make sure they are correct

# ID should be int type
# Age (years) should be int type
# fall asleep time should be TimestampType
# wake time should be TimestampType
df2 = df2[df2['ID'].apply(pd.to_numeric, errors='coerce').notna()] # Drop if ID string is not numeric
df2['ID'] = df2['ID'].astype(int) # Change ID type to integer
df2['Age (years)'] = df2['Age (years)'].astype(int) # Age (years)'s type should be int type
df2['fall asleep time'] = pd.to_datetime(df2['fall asleep time'], format= '%Y/%m/%d %H:%M' ) # fall asleep time should be TimestampType
df2['wake time'] = pd.to_datetime(df2['wake time'], format= '%Y/%m/%d %H:%M' ) # wake time should be TimestampType

print(df2.dtypes) # check all columns' types are successfully change to the correct one

schema = StructType([ \
    StructField("ID",IntegerType(),True), \
    StructField("Sex",StringType(),True), \
    StructField("Age (years)",IntegerType(),True), \
    StructField("ECG Complete", StringType(), True), \
    StructField("fall asleep time", TimestampType(), True), \
    StructField("wake time", TimestampType(), True), \
    StructField("Signal quality",StringType(),True), \
    StructField("Time of data collection (h)",FloatType(),True), \
    StructField("Time in bed (h)",FloatType(),True), \
    StructField("Sleep duration (h)\nReference value: \n7.0-9.0 hours", FloatType(), True), \
    StructField("Deep sleep (h)\nReference value: \n2.8-4.4 hours", FloatType(), True), \
    StructField("Light sleep (h)\nReference value: 2.0-2.8 hours",FloatType(),True), \
    StructField("REM (h)\nReference value: \n1.4-2.0 hours",FloatType(),True), \
    StructField("Wake (h)\nReference value: \n<= 0.4 hours",FloatType(),True), \
    StructField("Deep sleep onset (min)\nReference value: \n<= 30 minutes", FloatType(), True), \
    StructField("Sleep efficiency\nReference value: \n>= 0.9", FloatType(), True), \
    StructField("Apnea index (/hour)\nReference value: \n< 5/hour", FloatType(), True), \
    StructField("Apnea type\nC: central; \nO: obstructive", StringType(), True), \
    StructField("Remarks",StringType(),True) \
  ])

df3=spark.createDataFrame(data=df2, schema=schema)
df3.printSchema()

df4 = df3.withColumnRenamed("Age (years)","Age") \
    .withColumnRenamed("ECG Complete","ECG_Complete") \
    .withColumnRenamed("fall asleep time","fall_asleep_time") \
    .withColumnRenamed("wake time","wake_time") \
    .withColumnRenamed("Signal quality","Signal_quality") \
    .withColumnRenamed("Time of data collection (h)","Time_of_data_collection") \
    .withColumnRenamed("Time in bed (h)","Time_in_bed") \
    .withColumnRenamed("Sleep duration (h)\nReference value: \n7.0-9.0 hours","Sleep_duration") \
    .withColumnRenamed("Deep sleep (h)\nReference value: \n2.8-4.4 hours","Deep_sleep") \
    .withColumnRenamed("Light sleep (h)\nReference value: 2.0-2.8 hours","Light_sleep") \
    .withColumnRenamed("REM (h)\nReference value: \n1.4-2.0 hours","REM") \
    .withColumnRenamed("Wake (h)\nReference value: \n<= 0.4 hours","Wake") \
    .withColumnRenamed("Deep sleep onset (min)\nReference value: \n<= 30 minutes","Deep_sleep_onset") \
    .withColumnRenamed("Sleep efficiency\nReference value: \n>= 0.9","Sleep_efficiency") \
    .withColumnRenamed("Apnea index (/hour)\nReference value: \n< 5/hour","Apnea_index") \
    .withColumnRenamed("Apnea type\nC: central; \nO: obstructive","Apnea_type")

df4.coalesce(1).write.option("header", "true").option("index", "false").format("csv").save("/user/ytk244/final_project/dataset/sleepCsvOutput")