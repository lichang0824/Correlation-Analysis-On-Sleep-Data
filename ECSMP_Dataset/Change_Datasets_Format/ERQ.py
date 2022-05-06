import pandas as pd # Permission from professor for using pandas
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType

spark = SparkSession.builder.getOrCreate()
ERQ_df = pd.read_excel(open("/home/ytk244/Final_Project/BD_Original_Datasets/scale.xlsx", 'rb'), sheet_name='ERQ', engine="openpyxl") 
ERQ_df = ERQ_df[ERQ_df['ID'].apply(pd.to_numeric, errors='coerce').notna()] # Drop if ID string is not numeric
ERQ_df['ID'] = ERQ_df['ID'].astype(int) # Change ID type to integer
ERQ_df['Age (years)'] = ERQ_df['Age (years)'].astype(int) # Age (years)'s type should be int type
ERQ_df['Date'] = pd.to_datetime(ERQ_df['Date'], format= '%Y/%m/%d %H:%M' ) # want the Date column to be "date" type instead of object type

new_ERQ_df=spark.createDataFrame(data=ERQ_df)
new_ERQ_df.coalesce(1).write.option("header", "true").option("index", "false").format("csv").save("/user/ytk244/final_project/dataset/ERQOutput")