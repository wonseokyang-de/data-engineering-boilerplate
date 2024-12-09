import time
import logging

from airflow.sensors.base import BaseSensorOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


    template_fields = ('instance_name',)
    ui_color = "#4B863E"

    def __init__(
        self,
        instance_name: str,
        instance_status: str = 'InService',
        status_checking_seconds: int = 15,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.instance_status = instance_status
        self.status_checking_seconds = status_checking_seconds

    def poke(self, context):
        sagemaker_client = AwsBaseHook(
            client_type='sagemaker', 
            region_name='ap-northeast-2'
        ).get_client_type()

        logging.info(f"""
        ---ARGS: 
            instance_name: {self.instance_name}
            instance_status: {self.instance_status}
            status_checking_seconds: {self.status_checking_seconds}
        """)
        while True:
            instance_info = sagemaker_client.list_notebook_instances(
                NameContains=self.instance_name
            )['NotebookInstances'][0]
            _instance_status = instance_info['NotebookInstanceStatus']

            if _instance_status == self.instance_status:
                logging.info(f"[SUCCESS] - Instance Name: {self.instance_name} | Status: {_instance_status}")
                return True
            elif _instance_status != 'Pending':
                logging.error(f"Instance Name: {self.instance_name} | Status: {_instance_status}")
                return False

            logging.info(f"[SLEEP({self.status_checking_seconds})s] - Instance Name: {self.instance_name} | Status: {_instance_status}")
            time.sleep(self.status_checking_seconds)
