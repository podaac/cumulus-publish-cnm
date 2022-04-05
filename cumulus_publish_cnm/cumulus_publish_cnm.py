import json
import boto3
from cumulus_logger import CumulusLogger
from cumulus_process import Process

logger = CumulusLogger('publish_cnm_logger')


class PublishCNM(Process):
    className = 'PublishCNM'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logger
        self.logger.debug('{} Entered __init__', self.className)

        required = ['sns_endpoint']

        for requirement in required:
            if requirement not in self.config.keys():
                raise Exception(f'{requirement} config key is missing')

    def process(self):
        # config
        meta_sns_endpoint = self.config.get('sns_endpoint', '')

        self.logger.debug('sns_endpoint: {}', meta_sns_endpoint)

        sns = boto3.resource('sns')
        # Trigger SNS into ingestion stream (for each granule)
        platform_endpoint = sns.PlatformEndpoint(meta_sns_endpoint)
        response = []

        for item in self.input['cnm_list']:
            resp = platform_endpoint.publish(
                Message=json.dumps(item)
            )
            response.append(resp)
        logger.debug(json.dumps(self.input))

        return response


def lambda_handler(event, context):
    logger.setMetadata(event, context)
    return PublishCNM.cumulus_handler(event, context=context)
