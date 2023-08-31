from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)

# Replace these with your correct values
JOB_ROLE_ARN = "arn:aws:iam::354294757369:role/AmazonEMR-ExecutionRole-1692852128233"
S3_LOGS_BUCKET = "emr-airflow-zayn"

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://emr-airflow-zayn/logs/"}
    },
}


# def spark_params():
#     params_dict = {
#         # just some params to tweak your execution
#         'spark.jars.packages', 'com.databricks:spark-redshift_2.11:3.0.0-preview1'
#     }
#     # in my test run I provided both (spark.submit.pyFiles)
#     # pyfiles = f"--py-files s3://<UTILS-AS-ZIP>.zip"
#     configs = ' '.join([f"--conf {key}={params_dict[key]}" for key in params_dict])
#     return f"{configs}"


with DAG(
    dag_id="emr_redshift_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_app",
        job_type="SPARK",
        release_label="emr-6.7.0",
        config={"name": "airflow-test"},
    )

    application_id = create_app.output

    job1 = EmrServerlessStartJobOperator(
        task_id="start_job_1",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://zynairflowbkt/jobs/readwrites3.py",
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job2 = EmrServerlessStartJobOperator(
        task_id="start_job_2",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://zynairflowbkt/jobs/main.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job3 = EmrServerlessStartJobOperator(
        task_id="write_to_redshift_job_3",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://zynairflowbkt/jobs/writeToRedshift.py",
                "entryPointArguments": ["1000"],
                "sparkSubmitParameters": "--jars s3://zynairflowbkt/jobs/redshift-jdbc42-2.1.0.9.jar"
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )



    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        trigger_rule="all_done",
    )

    #(create_app >> [job1, job2, job3] >> delete_app)
    (create_app >> [job3] >> delete_app)