import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {"owner": "Airflow", "start_date": days_ago(2)}

# Define the DAG
dag = DAG(
    dag_id="ELECTRIC_VEHICLES_ETL_DAG",
    default_args=default_args,
    schedule_interval=None
)

# EMR Cluster configuration
JOB_FLOW_OVERRIDES = {
    'Name': 'transient_demo_testing',
    'ReleaseLabel': 'emr-5.33.0',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': "Master",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': "Slave",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            }
        ],
        'Ec2KeyName': '',  # Add your EC2 Key Name here
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-0b888ea278bbdd71d',
    },
    'LogUri': 's3://{}/'.format('your-log-bucket-name'),  # Replace with your log bucket
    'BootstrapActions': [],
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hive'}],
}

#get sql script from S3
def get_sql_from_s3(**kwargs):
    s3 = boto3.client('s3')
    bucket_name = 'washigton-electricvehicles-data-analysis-spark-etl'
    s3_key = 'scripts/src/snowflake.sql.'

    response = s3.get_object(Bucket=bucket_name, Key=s3_key)
    sql_content = response['Body'].read().decode('utf-8')
    
    # Store the SQL content in XCom for use in the next task
    kwargs['ti'].xcom_push(key='sql_content', value=sql_content)

# Define the DAG tasks
with dag:
    # Create EMR Cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
    )

    # Add step to run pytest on test.py
    pytest_step = EmrAddStepsOperator(
        task_id='add_pytest_step',
        job_flow_id=create_emr_cluster.output,
        steps=[
            {
                'Name': 'Run Pytest',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'sh', '-c',
                        'pip install pytest && '
                        'aws s3 cp s3://washigton-electricvehicles-data-analysis-spark-etl/scripts/tests/transform_test.py /home/hadoop/ && '
                        'aws s3 cp s3://washigton-electricvehicles-data-analysis-spark-etl/scripts/src/transform.py /home/hadoop/ && '
                        'pytest /home/hadoop/transform_test.py'
                    ],
                },
            }
        ],
        aws_conn_id='aws_default',
    )

    # Wait for pytest step to complete
    wait_for_pytest_step = EmrStepSensor(
        task_id='wait_for_pytest_step',
        job_flow_id=create_emr_cluster.output,
        step_id=pytest_step.output,
        aws_conn_id='aws_default',
    )

    # Add step to run Spark transform layer
    transform_layer_step = EmrAddStepsOperator(
        task_id='add_transform_layer_step',
        job_flow_id=create_emr_cluster.output,
        steps=[
            {
                'Name': 'Transform Layer',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--master', 'yarn',
                        '--deploy-mode', 'cluster',
                        's3://washigton-electricvehicles-data-analysis-spark-etl/scripts/src/transform.py'
                    ],
                },
            }
        ],
        aws_conn_id='aws_default',
    )

    # Wait for transform layer step to complete
    wait_for_transform_layer_step = EmrStepSensor(
        task_id='wait_for_transform_layer_step',
        job_flow_id=create_emr_cluster.output,
        step_id=transform_layer_step.output,
        aws_conn_id='aws_default',
    )

    # Terminate EMR Cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id=create_emr_cluster.output,
        aws_conn_id='aws_default',
    )

    #fetch sql from snowflake script in S3
    fetch_sql_task = PythonOperator(
        task_id='fetch_sql_from_s3',
        python_callable=get_sql_from_s3,
        provide_context=True
    )

    # Snowflake Load Task
    snowflake_load = SnowflakeOperator(
        task_id="snowflake_load",
        sql="{{ ti.xcom_pull(task_ids='fetch_sql_from_s3', key='sql_content') }}",
        snowflake_conn_id="snowflake_conn"
    )

# Task dependencies
create_emr_cluster >> pytest_step >> wait_for_pytest_step >> transform_layer_step >> wait_for_transform_layer_step >> terminate_emr_cluster >>fetch_sql_task>> snowflake_load
