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

    subscriber = spark.read.option("header", "true").csv("s3://capstonprojecttakeo/capstonData/subscriber.csv")

    has_nulls = subscriber.dropna().count() < subscriber.count()
    if has_nulls:
        print("The dataset has null values.")
    else:
        print("The dataset does not have null values.")

    subscriber = subscriber.withColumnRenamed("sub _id", "sub_id")
    subscriber = subscriber.withColumnRenamed("Zip Code", "ZipCode")

    null_sums = []

    for col_name in subscriber.columns:
        null_sum = subscriber.filter(col(col_name).isNull()).count()
        null_sums.append((col_name, null_sum))

    print("Sum of null values in each column:")
    for col_name, null_sum in null_sums:
        print(f'Column "{col_name}": {null_sum}')

    subscriber = subscriber.na.fill("NA", subset=["first_name"])
    subscriber = subscriber.na.fill("NA", subset=["Phone"])
    subscriber = subscriber.na.fill("NA", subset=["Subgrp_id"])
    subscriber = subscriber.na.fill("NA", subset=["Elig_ind"])

    subscriber.write.format("redshift")\
        .option("url","jdbc:redshift://default-workgroup.257240804294.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
        .option("dbtable", "public.subscribe")\
        .option("user", "admin")\
        .option("password", "Nepal123")\
        .option("tempdir", "s3a://capstonprojecttakeo/tmp/")\
        .option("aws_iam_role", "arn:aws:iam::257240804294:role/RedShiftAdmin").mode("overwrite").save()