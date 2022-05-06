import pandas as pd
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType

spark = SparkSession.builder.getOrCreate()
POMS_df = pd.read_excel(open("/home/ytk244/Final_Project/BD_Original_Datasets/scale.xlsx", 'rb'), sheet_name='POMS', engine="openpyxl")
POMS_df = POMS_df[POMS_df['ID'].apply(pd.to_numeric, errors='coerce').notna()] # Drop if ID string is not numeric
POMS_df['ID'] = POMS_df['ID'].astype(int) # Change ID type to integer
POMS_df['Age (years)'] = POMS_df['Age (years)'].astype(int) # Age (years)'s type should be int type
POMS_df['Date'] = pd.to_datetime(POMS_df['Date'], format= '%Y/%m/%d %H:%M' ) # want the Date column to be "date" type instead of object type

new_POMS_df=spark.createDataFrame(data=POMS_df)
new_POMS_df.coalesce(1).write.option("header", "true").option("index", "false").format("csv").save("/user/ytk244/final_project/dataset/POMSOutput")