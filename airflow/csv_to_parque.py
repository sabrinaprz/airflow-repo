from pyspark.sql import SparkSession


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    spark: SparkSession = SparkSession.builder.getOrCreate()
    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
    #   spark: SparkSession = SparkSession.builder.config("spark.jars.packages", "com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:3.3.3").master("local[*]").appName(
    #       "SparkByExamples.com").getOrCreate()
    #
    df = spark.read.options(header='True', inferSchema='True').csv("s3://glue-zayn/bootcamp/orders/orders.csv")
    df.write.parquet("s3://zayn-emr/capstone/airflow_out/order_par/")
