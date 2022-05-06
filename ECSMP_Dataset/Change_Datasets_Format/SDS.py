import pandas as pd
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, TimestampType, FloatType

spark = SparkSession.builder.getOrCreate()
df2 = pd.read_excel(open("/home/ytk244/Final_Project/BD_Original_Datasets/scale.xlsx", 'rb'), sheet_name='SDS', engine="openpyxl")
print(df2.dtypes) # check each column's datatype to make sure they are correct
df2 = df2[df2['ID'].apply(pd.to_numeric, errors='coerce').notna()] # Drop if ID string is not numeric
df2['Date'] = pd.to_datetime(df2['Date'], format= '%Y/%m/%d %H:%M' ) # want the Date column to be "date" type instead of object type

schema = StructType([ \
    StructField("ID",IntegerType(),True), \
    StructField("Sex",StringType(),True), \
    StructField("Age (years)",IntegerType(),True), \
    StructField("Complete", StringType(), True), \
    StructField("Date", TimestampType(), True), \
    StructField("1. I feel down-hearted and blue.", FloatType(), True), \
    StructField("2. Morning is when I feel the best.", FloatType(), True), \
    StructField("3. I have crying spells or feel like it.", FloatType(), True), \
    StructField("4. I have trouble sleeping at night.", FloatType(), True), \
    StructField("5. I eat as much as I used to.", FloatType(), True), \
    StructField("6. I still enjoy sex.", FloatType(), True), \
    StructField("7. I notice that I am losing weight.", FloatType(), True), \
    StructField("8. I have trouble with constipation.", FloatType(), True), \
    StructField("9. My heart beats faster than usual.", FloatType(), True), \
    StructField("10. I get tired for no reason.", FloatType(), True), \
    StructField("11. My mind is as clear as it used to be.", FloatType(), True), \
    StructField("12. I find it easy to do the things I used to.", FloatType(), True), \
    StructField("13. I am restless and can’t keep still.", FloatType(), True), \
    StructField("14. I feel hopeful about the future.", FloatType(), True), \
    StructField("15. I am more irritable than usual.", FloatType(), True), \
    StructField("16. I find it easy to make decisions.", FloatType(), True), \
    StructField("17. I feel that I am useful and needed.", FloatType(), True), \
    StructField("18. My life is pretty full.", FloatType(), True), \
    StructField("19. I feel that others would be better off if I were dead.", FloatType(), True), \
    StructField("20. I still enjoy the things I used to do.", FloatType(), True), \
    StructField("SDS Score (score * 1.25) (2, 5, 6, 11, 12, 14, 16, 17, 18, and 20 are reversed)",FloatType(),True) \
  ])

df3=spark.createDataFrame(data=df2, schema=schema)
df3.coalesce(1).write.option("header", "true").option("index", "false").format("csv").save("/user/ytk244/final_project/dataset/SDSOutput")