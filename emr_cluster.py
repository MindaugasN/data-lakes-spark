import os
import boto3


class EMRLoader:
    def __init__(self, cluster_name, aws_access_key, aws_secret_access_key, region_name, key_name, 
                bucket, logs_prefix, scripts_prefix):
        self.key_name = key_name
        self.cluster_name = cluster_name
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        self.bucket = bucket
        self.logs_prefix = logs_prefix
        self.scripts_prefix = scripts_prefix
    
        self.boto_client = self._set_client('emr')
        self.s3_client = self._set_client('s3')
    
    def _set_client(self, service_name):
        client = boto3.client(service_name,
                              aws_access_key_id=self.aws_access_key,
                              aws_secret_access_key=self.aws_secret_access_key,
                              region_name=self.region_name)
        return client


    def start_cluster(self):
        logs_uri = f's3://{os.path.join(self.bucket, self.logs_prefix)}'

        response = self.boto_client.run_job_flow(
            Name=self.cluster_name,
            LogUri=logs_uri,
            ReleaseLabel='emr-5.29.0',
            Instances={
                'MasterInstanceType': 'm5.xlarge',
                'SlaveInstanceType': 'm5.xlarge',
                'InstanceCount': 3,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': self.key_name
            },
            Applications=[
                {'Name': 'Hadoop'},
                {'Name': 'Spark'},
                {'Name': 'Hive'},
                {'Name': 'Livy'},
                {'Name': 'Zeppelin'}
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
        
        self.current_emr_cluster_id = response['JobFlowId']
        
        return response

    def setup_script(self, script_name, local_scripts_folder='spark_scripts'):
        script_uri = f's3://{os.path.join(self.bucket, self.scripts_prefix, script_name)}'

        self.s3_client.upload_file(
            os.path.join(local_scripts_folder, script_name),
            self.bucket, os.path.join(self.scripts_prefix, script_name)
        )

        response = self.boto_client.add_job_flow_steps(
            JobFlowId=self.current_emr_cluster_id,
            Steps=[{
                'Name': 'Load Spark script into hadoop',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', script_uri, '/home/hadoop/']
                }
            }]
        )

        return response

    def run_spark_job(self, script_name):
        response = self.boto_client.add_job_flow_steps(
            JobFlowId=self.current_emr_cluster_id,
            Steps=[{
                'Name': 'Run Custom Spark Job',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '/home/hadoop/' + script_name]
                }
            }]
        )

        return response
    
    def terminate_cluster(self):
        response = self.boto_client.terminate_job_flows(
            JobFlowIds=[self.current_emr_cluster_id]
        )
        
        print(f'Terminating cluster {self.current_emr_cluster_id}')
        
        return response