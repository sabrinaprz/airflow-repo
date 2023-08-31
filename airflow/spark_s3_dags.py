
from pyspark.sql import SparkSession

try:
    import airflow
    from datetime import datetime, timedelta
    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.python_operator import PythonOperator


except Exception as e:
    print("Error  {} ".format(e))


def processo_etl_spark():

    spark: SparkSession = SparkSession.builder.config("spark.jars.packages", "com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:3.3.3").master("local[*]").appName(
        "SparkByExamples.com").getOrCreate()
    df = spark.read.options(header='True', inferSchema='True').csv("s3a://glue-zayn/bootcamp/orders/orders.csv")
    df.write.parquet("s3a://zayn-emr/capstone/airflow_out/order_par/")


default_args = {
    'owner': 'zayn',
    'start_date': datetime(2023, 7, 7),
    'retries': 1,
    'retry_delay': timedelta(hours=1)
}
with airflow.DAG(dag_id="spark_s3_dag",
                 default_args=default_args,
                 schedule_interval="@once") as dag:
    spark_s3 = PythonOperator(
        task_id='spark_s3',
        python_callable=processo_etl_spark
    )

spark_s3
