import pandas as pd
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
PSQI_df = pd.read_excel(open("/home/ytk244/Final_Project/BD_Original_Datasets/scale.xlsx", 'rb'), sheet_name='PSQI', engine="openpyxl")
PSQI_df = PSQI_df[PSQI_df['ID'].apply(pd.to_numeric, errors='coerce').notna()] # Drop if ID string is not numeric
PSQI_df = PSQI_df.fillna(0)
PSQI_df['ID'] = PSQI_df['ID'].astype('Int64') # Change ID type to integer
PSQI_df['Age (years)'] = PSQI_df['Age (years)'].astype('Int64') # Age (years)'s type should be int type
PSQI_df['Date'] = pd.to_datetime(PSQI_df['Date'], format= '%Y/%m/%d %H:%M' ) # want the Date column to be "date" type instead of object type
PSQI_df.iloc[:,6] = PSQI_df.iloc[:,6].astype('Int64')
PSQI_df.iloc[:,9] = PSQI_df.iloc[:,9].astype('Int64')
PSQI_df.iloc[:,10] = PSQI_df.iloc[:,10].astype('Int64')
PSQI_df.iloc[:,11] = PSQI_df.iloc[:,11].astype('Int64')
PSQI_df.iloc[:,12] = PSQI_df.iloc[:,12].astype('Int64')
PSQI_df.iloc[:,13] = PSQI_df.iloc[:,13].astype('Int64')
PSQI_df.iloc[:,14] = PSQI_df.iloc[:,14].astype('Int64')
PSQI_df.iloc[:,15] = PSQI_df.iloc[:,15].astype('Int64')
PSQI_df.iloc[:,16] = PSQI_df.iloc[:,16].astype('Int64')
PSQI_df.iloc[:,17] = PSQI_df.iloc[:,17].astype('Int64')
PSQI_df.iloc[:,18] = PSQI_df.iloc[:,18].astype('Int64')
PSQI_df.iloc[:,20] = PSQI_df.iloc[:,20].astype('Int64')
PSQI_df.iloc[:,21] = PSQI_df.iloc[:,21].astype('Int64')
PSQI_df.iloc[:,22] = PSQI_df.iloc[:,22].astype('Int64')
PSQI_df.iloc[:,23] = PSQI_df.iloc[:,23].astype('Int64')
PSQI_df.iloc[:,24] = PSQI_df.iloc[:,24].astype('Int64')
PSQI_df.iloc[:,25] = PSQI_df.iloc[:,25].astype('Int64')
PSQI_df.iloc[:,26] = PSQI_df.iloc[:,26].astype('Int64')
PSQI_df.iloc[:,27] = PSQI_df.iloc[:,27].astype('Int64')
PSQI_df.iloc[:,28] = PSQI_df.iloc[:,28].astype('Int64')
PSQI_df.iloc[:,29] = PSQI_df.iloc[:,29].astype('Int64')
PSQI_df.iloc[:,30] = PSQI_df.iloc[:,30].astype('Int64')
PSQI_df.iloc[:,31] = PSQI_df.iloc[:,31].astype('Int64')

schema = StructType([ \
    StructField("ID",IntegerType(),True), \
    StructField("Sex",StringType(),True), \
    StructField("Age (years)",IntegerType(),True), \
    StructField("Complete",StringType(),True), \
    StructField("Date",TimestampType(),True), \
    StructField("1. During the past month, what time have you usually gone to bed at night?",StringType(),True), \
    StructField("2. During the past month, how long (in minutes) has it usually taken you to fall asleep each night?",IntegerType(),True), \
    StructField("3. During the past month, what time have you usually gotten up in the morning? ",StringType(),True), \
    StructField("4. During the past month, how many hours of actual sleep did you get at night? (This may be different than the number of hours you spent in bed.)",StringType(),True), \
    StructField("5. During the past month, how often have you had trouble sleeping because you： a. Cannot get to sleep within 30 minutes",IntegerType(),True), \
    StructField("b. Wake up in the middle of the night or early morning",IntegerType(),True), \
    StructField("c. Have to get up to use the bathroom",IntegerType(),True), \
    StructField("d. Cannot breathe comfortably",IntegerType(),True), \
    StructField("e. Cough or snore loudly",IntegerType(),True), \
    StructField("f. Feel too cold",IntegerType(),True), \
    StructField("g. Feel too hot"	,IntegerType(),True), \
    StructField("h. Have bad dreams",IntegerType(),True), \
    StructField("i. Have pain",IntegerType(),True), \
    StructField("j. Other reason (s), please describe, including how often you have had trouble sleeping because of this reason (s):",IntegerType(),True), \
    StructField("reason",StringType(),True), \
    StructField("6. During the past month, how often have you taken medicine (prescribed or “over the counter”) to help you sleep?",IntegerType(),True), \
    StructField("7. During the past month, how often have you had trouble staying awake while driving, eating meals, or engaging in social activity?",IntegerType(),True), \
    StructField("8. During the past month, how much of a problem has it been for you to keep up enthusiasm to get things done?",IntegerType(),True), \
    StructField("9. During the past month, how would you rate your sleep quality overall?",IntegerType(),True), \
    StructField("A. Sleep Quality",IntegerType(),True), \
    StructField("B. Sleep Latency",IntegerType(),True), \
    StructField("C. Sleep Duration",IntegerType(),True), \
    StructField("D. Sleep Efficiency",IntegerType(),True), \
    StructField("E. Sleep Disorders",IntegerType(),True), \
    StructField("F. Hypnotic",IntegerType(),True), \
    StructField("G. Daytime Dysfunction",IntegerType(),True), \
    StructField("PSQI",IntegerType(),True), \
  ])

new_PSQI_df=spark.createDataFrame(data=PSQI_df, schema=schema)
new_PSQI_df.coalesce(1).write.option("header", "true").option("index", "false").format("csv").save("/user/ytk244/final_project/dataset/PSQIOutput")