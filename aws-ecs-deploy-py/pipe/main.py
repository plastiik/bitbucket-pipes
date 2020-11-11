import json
from pprint import pformat

import yaml
import boto3
from bitbucket_pipes_toolkit import Pipe, get_logger
from botocore.exceptions import ClientError, ParamValidationError, WaiterError

logger = get_logger()

schema = {
    "AWS_ACCESS_KEY_ID": {
        "type": "string",
        "required": True
    },
    "AWS_SECRET_ACCESS_KEY": {
        "type": "string",
        "required": True
    },
    "AWS_DEFAULT_REGION": {
        "type": "string",
        "required": True
    },
    "AWS_DEFAULT_PROFILE": {
        "type": "string",
        "required": True
    },
    "CLUSTER_NAME": {
        "type": "string",
        "required": True
    },
    "SERVICE_NAME": {
        "type": "string",
        "required": True
    },
    "TASK_DEFINITION": {
        "type": "string",
        "required": True
    },
    "FORCE_NEW_DEPLOYMENT": {
        "type": "boolean",
        "default": False
    },
    "WAIT": {
        "type": "boolean",
        "default": False
    },
    "DEBUG": {
        "type": "boolean",
        "default": False
    }
}


class ECSDeploy(Pipe):

    def get_client(self):
        try:
            return boto3.client('ecs', region_name=self.get_variable('AWS_DEFAULT_REGION' profile_name=self.get_variable('AWS_DEFAULT_PROFILE'))
        except ClientError as err:
            self.fail("Failed to create boto3 client.\n" + str(err))

    def _handle_update_service_error(self, error):
        error_code = error.response['Error']['Code']
        if error_code == 'ClusterNotFoundException':
            msg = f'ECS cluster not found. Check your CLUSTER_NAME.'
        elif error_code == 'ServiceNotFoundException':
            msg = f'ECS service not found. Check your SERVICE_NAME'
        else:
            msg = f"Failed to update the stack.\n" + str(error)
        self.fail(msg)

    def update_task_definition(self, task_definition_file, image=None):

        logger.info(f'Updating the task definition...')

        client = self.get_client()
        try:
            with open(task_definition_file) as d_file:
                task_definition = json.load(d_file)
        except json.decoder.JSONDecodeError:
            self.fail('Failed to parse the task definition file: invalid JSON provided.')
        except FileNotFoundError:
            self.fail(f'Not able to find {task_definition_file} in your repository.')

        logger.info(f'Using task definition: \n{pformat(task_definition)}')

        try:
            response = client.register_task_definition(**task_definition)
            return response['taskDefinition']['taskDefinitionArn']
        except ClientError as err:
            self.fail("Failed to update the stack.\n" + str(err))
        except KeyError as err:
            self.fail("Unable to retrieve taskDefinitionArn key.\n" + str(err))
        except ParamValidationError as err:
            self.fail(f"ECS task definition parameter validation error: \n {err.args[0]}")

    def update_service(self, cluster, service, task_definition, force_new_deployment=False):

        logger.info(f'Update the {service} service.')

        client = self.get_client()

        try:
            response = client.update_service(
                cluster=cluster,
                service=service,
                taskDefinition=task_definition,
                forceNewDeployment=force_new_deployment
            )
            return response
        except ClientError as err:
            self._handle_update_service_error(err)

    def run(self):
        super().run()
        region = self.get_variable('AWS_DEFAULT_REGION')
        definition = self.get_variable('TASK_DEFINITION')
        try:
            image = self.get_variable('IMAGE_NAME')
        except KeyError:
            image = None
        cluster_name = self.get_variable('CLUSTER_NAME')
        service_name = self.get_variable('SERVICE_NAME')
        force_new_deployment = self.get_variable('FORCE_NEW_DEPLOYMENT')

        task_definition = self.update_task_definition(definition, image)
        response = self.update_service(
            cluster_name, service_name, task_definition, force_new_deployment)

        logger.debug(response)

        if self.get_variable('WAIT'):
            logger.info('Waiting for service to become Stable...')

            client = self.get_client()
            waiter = client.get_waiter('services_stable')
            try:
                waiter.wait(cluster=cluster_name, services=[service_name])
            except WaiterError as e:
                self.fail(f'Error waiting for service to become stable: {e}')
            self.log_info(f'Service {service_name} has become stable')

        self.success(f'Successfully updated the {service_name} service. You can check you service here: \n'
                     f'https://console.aws.amazon.com/ecs/home?region={region}#/clusters/{cluster_name}/services/{service_name}/details')


if __name__ == '__main__':
    with open('/usr/bin/pipe.yml', 'r') as metadata_file:
        metadata = yaml.safe_load(metadata_file.read())
    pipe = ECSDeploy(pipe_metadata=metadata, schema=schema, check_for_newer_version=True)
    pipe.run()
