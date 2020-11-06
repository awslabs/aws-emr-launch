# Airflow 1.x Plugin and Example DAG

This directory includes an Airflow 1.x Plugin with Operators, Sensors, and Hooks to enable Airflow to interact with AWS Step Functions. A simple example DAG making use of these classes is also provided.

By enabling this interaction, AWS EMR Launch can be used to define the EMR Clusters and Airflow can launch and orchestrate jobs running on the clusters.

This example reads metadata about the job from the AWS Parameter Store using Airflow's `SystemsManagerParameterStoreBackend`.

For Airflow 2.x, Operators, Sensors, and Hooks are included with the Airflow deployment so no plugin is necesary.