import logging

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

class GlueJobWithLoggingOperator(GlueJobOperator):
    def pre_execute(self, context=None):
        logging.info("Preparing to run Glue Job with params")
        logging.info("\t Job Name: {self.job_name}")
        logging.info("\t Script Args: {self.script_args}")
        logging.info("\t Job Run kwargs: {self.run_job_kwargs}")