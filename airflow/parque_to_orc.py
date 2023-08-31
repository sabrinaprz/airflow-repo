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
    df = spark.read.options(header='True', inferSchema='True').parquet("s3://takeo1/data.parquet")
    df.write.orc("s3://takeo1/data.orc")
