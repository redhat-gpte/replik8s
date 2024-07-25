import kubernetes_asyncio
import os

from aioshutil import rmtree
from tempfile import mkdtemp

class Replik8s():
    api_group = os.environ.get('API_GROUP', 'replik8s.rhpds.redhat.com')
    api_version = os.environ.get('API_VERSION', 'v1')

    @classmethod
    async def on_cleanup(cls):
        await cls.api_client.close()
        if cls.data_dir_is_temp:
            await rmtree(cls.data_dir)

    @classmethod
    async def on_startup(cls):
        if os.path.exists('/run/secrets/kubernetes.io/serviceaccount'):
            kubernetes_asyncio.config.load_incluster_config()
            with open('/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
                cls.namespace = f.read()
        else:
            await kubernetes_asyncio.config.load_kube_config()
            if 'OPERATOR_NAMESPACE' in os.environ:
                cls.namespace = os.environ['OPERATOR_NAMESPACE']
            else:
                raise Exception(
                    'Unable to determine operator namespace. '
                    'Please set OPERATOR_NAMESPACE environment variable.'
                )

        cls.api_client = kubernetes_asyncio.client.ApiClient()
        cls.core_v1_api = kubernetes_asyncio.client.CoreV1Api(cls.api_client)
        cls.custom_objects_api = kubernetes_asyncio.client.CustomObjectsApi(cls.api_client)

        if 'DATA_DIR' in os.environ:
            cls.data_dir = os.environ['DATA_DIR']
            cls.data_dir_is_temp = False
        else:
            cls.data_dir = mkdtemp(prefix='replik8s-data-')
            cls.data_dir_is_temp = True
