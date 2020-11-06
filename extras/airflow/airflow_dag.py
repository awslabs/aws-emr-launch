
import json

from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.secrets.aws_systems_manager import SystemsManagerParameterStoreBackend
from airflow.utils.dates import days_ago

from airflow.operators.aws_operators_plugin import StepFunctionStartExecutionOperator
from airflow.sensors.aws_operators_plugin import StepFunctionExecutionSensor


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}


with DAG(
        dag_id='example_dag',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        schedule_interval='0 3 * * *',
        tags=['example'],
) as dag:

    # Load DAG Parameters from Parameter Store value deployed by CDK Stack
    parameter_store_backend = SystemsManagerParameterStoreBackend()
    dag_parameters = json.loads(parameter_store_backend.get_conn_uri('example/dag'))
    state_machine_arn = dag_parameters['StateMachineArn']
    input_path = dag_parameters['InputPath']
    output_path = dag_parameters['OutputPath']
    spark_jar = dag_parameters['SparkJar']
    metrics_properties = dag_parameters['MetricsProperties']

    # This should bu updated to calculate Start/End
    start_datetime = '202001010000'
    end_datetime = '202001010000'

    cluster_creator = StepFunctionStartExecutionOperator(
        task_id='create_cluster',
        state_machine_arn=state_machine_arn,
        input={
            'ClusterConfigurationOverrides': {
                'CoreInstanceCount': 10
            }
        },
        aws_conn_id='aws_default'
    )

    cluster_checker = StepFunctionExecutionSensor(
        task_id='watch_create_cluster',
        execution_arn="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        aws_conn_id='aws_default'
    )

    job_flow_id="{{ task_instance.xcom_pull(task_ids='watch_create_cluster', key='output')['LaunchClusterResult']['ClusterId'] }}"

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id=job_flow_id,
        aws_conn_id='aws_default',
        steps=[
            {
                'Name': 'RunJob',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--master', 'yarn',
                        '--deploy-mode', 'cluster',
                        '--executor-cores', '4',
                        '--executor-memory', '10g',
                        '--driver-cores', '3',
                        '--driver-memory', '6g',
                        '--class', 'Example',
                        spark_jar,
                        '--files', metrics_properties,
                        '--conf', 'spark.metrics.conf=metrics.properties',
                        '--input', input_path,
                        '--start', start_datetime,
                        '--end', end_datetime,
                        '--output', output_path
                    ]
                }
            }
        ]
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=job_flow_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id=job_flow_id,
        aws_conn_id='aws_default',
        trigger_rule='all_done'
    )

    cluster_creator >> cluster_checker >> step_adder >> step_checker >> cluster_remover
