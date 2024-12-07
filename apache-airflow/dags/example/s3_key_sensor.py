from airflow.sensors.base import BaseSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3KeyRemovalSensor(BaseSensor):
    def __init__(
        bucket,
        key
    ):
        self.bucket = bucket
        self.key = key


    def poke(self):
        # TODO: check removal file
        ...
