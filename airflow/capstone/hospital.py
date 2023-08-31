#This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    spark: SparkSession = SparkSession.builder.getOrCreate()

    hospital = spark.read.option("header", "true").csv("s3://capstonprojecttakeo/capstonData/hospitals.csv")

    has_nulls = hospital.dropna().count() < hospital.count()
    if has_nulls:
        print("The dataset has null values.")
    else:
        print("The dataset does not have null values.")

    hospital.write.format("redshift")\
        .option("url","jdbc:redshift://default-workgroup.257240804294.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
        .option("dbtable", "public.hospital")\
        .option("user", "admin").option("password", "Nepal123").option("tempdir","s3a://capstonprojecttakeo/tmp/")\
        .option("aws_iam_role", "arn:aws:iam::257240804294:role/RedShiftAdmin").mode("overwrite").save()