# This is a sample Python script.

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


    patient_record = spark.read.option("header", "true").csv("s3://capstonprojecttakeo/capstonData/Patient_records.csv")

    has_nulls = patient_record.dropna().count() < patient_record.count()
    if has_nulls:
        print("The dataset has null values.")
    else:
        print("The dataset does not have null values.")


    null_sums = []

    for col_name in patient_record.columns:
        null_sum = patient_record.filter(col(col_name).isNull()).count()
        null_sums.append((col_name, null_sum))

    print("Sum of null values in each column:")
    for col_name, null_sum in null_sums:
        print(f'Column "{col_name}": {null_sum}')

    patient_record = patient_record.na.fill("NA", subset=["Patient_name"])
    patient_record = patient_record.na.fill("NA", subset=["patient_phone"])

    patient_record.write.format("redshift")\
        .option("url","jdbc:redshift://default-workgroup.257240804294.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
        .option("dbtable", "public.patient_record").option("user", "admin")\
        .option("password", "Nepal123")\
        .option("tempdir","s3a://capstonprojecttakeo/tmp/")\
        .option("aws_iam_role", "arn:aws:iam::257240804294:role/RedShiftAdmin")\
        .mode("overwrite").save()