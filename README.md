# Udacity Data Engineer Course / Project 4: Data Pipelines with Airflow
## 1 Description
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.


## 2 Project Setup
### 2.1 Prerequisites
The following tools/packages/frameworks have to be installed on your system
- Docker with docker-compose
- python3 with boto3

### 2.2 Create aws infrastructure
1. copy ```aws.example.cfg``` and rename it to ```aws.cfg``` and add your aws key and secret.
2. run ```python3 iac/create_aws_infrastructure.py``` to create the aws infrastructure (iam role, redshift cluster). The script will end, when the cluster is completly created and active. This may take a while.


### 2.3 Run the project
1. run ```docker-compose up``` to start all needed containers for airflow
2. visit ```http://localhost:8080``` to access the ui of airlfow
3. add Airflow Connections for AWS
4. run the ```udac_example_dag``` in the airflow dag overview

### 2.3 Delete aws infrastructure
1. run ```python3 iac/delets_aws_infrastructure.py``` to delete the aws infrastructure (iam role, redshift cluster).