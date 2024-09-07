import json
import time
import logging
import requests
import websocket

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


def get_sagemaker_client():
    aws_hook = AwsBaseHook(client_type='sagemaker', region_name='ap-northeast-2')
    return aws_hook.get_client_type()


class SageMakerCreateInstanceOperator(BaseOperator):

    template_fields = ('instance_name',)
    ui_color = "#ddbb77"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        instance_name: str,
        instance_type: str,
        role_arn: str,
        lifecycle_config_name: str,
        direct_internet_access: str,
        volume_size_gb: int,
        default_code_repo: str,
        root_access: str,
        platform_identifier: str,
        security_group_ids: list,
        subnet_id: str,
        tags: list,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.instance_type = instance_type
        self.role_arn = role_arn
        self.lifecycle_config_name = lifecycle_config_name
        self.direct_internet_access = direct_internet_access
        self.volume_size_gb = volume_size_gb
        self.default_code_repo = default_code_repo
        self.root_access = root_access
        self.platform_identifier = platform_identifier
        self.security_group_ids = security_group_ids
        self.subnet_id = subnet_id
        self.tags = tags

    def execute(self, context):
        sagemaker_client = get_sagemaker_client()
        response = sagemaker_client.create_notebook_instance(
            NotebookInstanceName=self.instance_name,
            InstanceType=self.instance_type,
            RoleArn=self.role_arn,
            LifecycleConfigName=self.lifecycle_config_name,
            DirectInternetAccess=self.direct_internet_access,
            VolumeSizeInGB=self.volume_size_gb,
            DefaultCodeRepository=self.default_code_repo,
            RootAccess=self.root_access,
            PlatformIdentifier=self.platform_identifier,
            SecurityGroupIds=self.security_group_ids,
            SubnetId=self.subnet_id,
            Tags=self.tags
        )
        logging.info(f"SageMaker Notebook Instance {response}")


class SageMakerSubmitCmdOperator(BaseOperator):

    template_fields = ('instance_name', 'command',)
    ui_color = '#33B2FF'

    def __init__(
        self,
        instance_name: str,
        command: str,
        conda_env: str = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name
        self.command = command
        self.conda_env = conda_env

    def _connect_to_instance(self):
        sagemaker_client = get_sagemaker_client()
        instance_login_url = sagemaker_client.create_presigned_notebook_instance_url(
            NotebookInstanceName=self.instance_name
        )['AuthorizedUrl']

        req_sess = requests.Session()
        login_response = req_sess.get(instance_login_url)
        logging.info(f"Login Response: {login_response}")

        url_base = instance_login_url.partition('?')[0]
        url_protocol, url_host_address = url_base.split('://')
        ws_base_url = f'wss://{url_host_address}/terminals/websocket'

        terminal_name = req_sess.post(
            f'{url_base}/api/terminals',
            params={
                '_xsrf': req_sess.cookies['_xsrf']
            }
        ).json()['name']

        cookies = req_sess.cookies.get_dict()

        instance_connection = websocket.create_connection(
            f'{ws_base_url}/{terminal_name}',
            cookie='; '.join(['%s=%s' % (k, v) for k, v in cookies.items()]),
            header=['User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'],
            host=url_host_address,
            origin=url_base
        )
        return instance_connection

    def _submit_cmd(self, instance_connection, _command):
        def is_stdout_error(stdout: str):
            logging.info("Check Error Comparing")
            logging.info(f"{stdout}")

            stdout_lines = stdout.split('\n')
            stdout_line_len = len(stdout_lines)

            if stdout_line_len > 1:
                is_python_error = ('Error' in stdout_lines[-1].split(':')[0])
                if is_python_error:
                    logging.error("Python Script Error")
                    return False
            elif stdout_line_len == 1:
                is_file_error = (stdout_lines[-1].split(':')[-1].strip() == 'No such file or directory')
                is_python_file_error = (stdout_lines[-1].split(':')[-1].strip() == '[Errno 2] No such file or directory')
                if is_file_error:
                    logging.error("No such file or directory")
                    return False
                elif is_python_file_error:
                    logging.error("Python No such file or directory")
                    return False

            logging.info("Check Error Comparing END")
            return True

        cmd_head = _command.split(' ')[0]

        stdin_command = json.dumps(['stdin', _command + '\n'])
        instance_connection.send(stdin_command)

        wait_cmds = ['python', 'python3', 'pip']

        if cmd_head not in wait_cmds:
            std_stream, output = json.loads(instance_connection.recv())
            return True

        # wait for completion
        outputs = []
        while True:
            std_stream, output = json.loads(instance_connection.recv())

            if std_stream != 'stdout':
                continue

            output = output.strip()
            outputs.append(output)
            logging.info(output)

            is_end_output = output.split(' ')[-1] == 'sh-4.2$'
            if is_end_output:
                logging.info("Command END")
                break
            time.sleep(3)

        end_output = outputs[-2]
        job_status = is_stdout_error(end_output)

        return job_status

    def execute(self, context):
        instance_connection = self._connect_to_instance()
        logging.info("Connected")

        if self.conda_env:
            cmd_activate_conda_env = f'source /home/ec2-user/anaconda3/bin/activate {self.conda_env}'
            self._submit_cmd(instance_connection, cmd_activate_conda_env)

        try:
            self._submit_cmd(instance_connection, self.command)
            logging.info("Running Complete")

        except Exception as e:
            logging.error(e)
            logging.error(f"instance_connection: {instance_connection}")
            logging.error(f"command: {self.command}")

            raise AirflowException()


class SageMakerStopInstanceOperator(BaseOperator):
    """
    Send stopping command to SageMakerNotebookInstance.
    """

    template_fields = ('instance_name',)
    ui_color = "#E3C289"

    def __init__(
        self,
        instance_name: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name

    def execute(self, context):
        sagemaker_client = get_sagemaker_client()
        sagemaker_client.stop_notebook_instance(NotebookInstanceName=self.instance_name)


class SageMakerDeleteInstanceOperator(BaseOperator):
    """
    Send deleting command to SageMakerNotebookInstance.
    """

    template_fields = ('instance_name',)
    ui_color = "#E389AE"

    def __init__(
        self,
        instance_name: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.instance_name = instance_name

    def execute(self, context):
        sagemaker_client = get_sagemaker_client()
        sagemaker_client.delete_notebook_instance(NotebookInstanceName=self.instance_name)
