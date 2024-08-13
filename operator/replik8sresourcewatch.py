import asyncio
import logging

from datetime import datetime, timezone
from typing import Mapping, Optional, TypeVar

import inflection
import kopf
import kubernetes_asyncio

Replik8sReplicationSourceT = TypeVar('Replik8sReplicationSourceT', bound='Replik8sReplicationSource')

logger = logging.getLogger('resource_watcher')

class ResourceWatchFailedError(Exception):
    pass

class ResourceWatchRestartError(Exception):
    pass

class Replik8sResourceWatch():
    def __init__(self,
        api_version: str,
        kind: str,
        namespace: Optional[str],
        plural: str,
        source: Replik8sReplicationSourceT,
    ):
        self.api_version = api_version
        self.kind = kind
        self.namespace = namespace
        self.plural = plural
        self.source = source
        self.error = None
        self.state = "Init"
        self.task = None

    def __str__(self):
        if self.namespace:
            return f"resource watch for {self.api_version} {self.plural} in {self.namespace}"
        else:
            return f"resource watch for {self.api_version} {self.plural}"

    async def __get_method_kwargs(self):
        if '/' in self.api_version:
            group, version = self.api_version.split('/')
            kwargs = dict(group=group, plural=self.plural, version=version)
            if self.namespace:
                method = self.source().custom_objects_api.list_namespaced_custom_object
                kwargs['namespace'] = self.namespace
            else:
                method = self.source().custom_objects_api.list_cluster_custom_object
        else:
            kind = inflection.underscore(self.kind)
            if self.namespace:
                method = getattr(self.source().core_v1_api, f"list_namespaced_{kind}")
                kwargs = dict(namespace=self.namespace)
            else:
                try:
                    method = getattr(self.source().core_v1_api, f"list_{kind}")
                except AttributeError:
                    method = getattr(self.source().core_v1_api, f"list_{kind}_for_all_namespaces")
                kwargs = {}
        return method, kwargs

    async def start(self,
        logger: kopf.ObjectLogger,
    ):
        logger.info(f"Starting {self}")
        self.task = asyncio.create_task(self.watch())

    async def stop(self):
        if not self.task:
            return
        logger.info(f"Stopping {self}")
        self.task.cancel()
        await self.task
        self.task = None

    async def refresh(self):
        method, kwargs = await self.__get_method_kwargs()
        await self.__refresh(method, **kwargs)

    async def __refresh(self, method, **kwargs):
        _continue = None
        while True:
            obj_list = await method(**kwargs, _continue=_continue, limit=50)
            items = obj_list.get('items', []) if isinstance(obj_list, dict) else obj_list.items
            for obj in items:
                if not isinstance(obj, Mapping):
                    obj = self.source().api_client.sanitize_for_serialization(obj)
                # Weird that seralization does not always produce kind and apiVersion!
                if 'apiVersion' not in obj:
                    obj['apiVersion'] = self.api_version
                if 'kind' not in obj:
                    obj['kind'] = self.kind
                await self.source().handle_resource_event(event_type='GET', event_obj=obj)
            _continue = obj_list['metadata'].get('continue') if isinstance(obj_list, dict) else obj_list.metadata._continue
            if not _continue:
                break

    async def watch(self):
        method, kwargs = await self.__get_method_kwargs()
        try:
            self.state = 'Starting'
            try:
                await self.__refresh(method, **kwargs)
            except Exception as exception:
                self.state = 'Error'
                self.error = str(exception)
                raise
            while True:
                self.state = 'Watching'
                watch_start_dt = datetime.now(timezone.utc)
                try:
                    await self.__watch(method, **kwargs)
                except asyncio.CancelledError:
                    logger.info(f"{self} cancelled")
                    return
                except ResourceWatchRestartError as e:
                    logger.info(f"{self} restart: {e}")
                    watch_duration = (datetime.now(timezone.utc) - watch_start_dt).total_seconds()
                    if watch_duration < 10:
                        self.state = 'Error'
                        self.error = str(e)
                        await asyncio.sleep(10 - watch_duration)
                except ResourceWatchFailedError as e:
                    logger.warning(f"{self} failed: {e}")
                    watch_duration = (datetime.now(timezone.utc) - watch_start_dt).total_seconds()
                    if watch_duration < 60:
                        self.state = 'Error'
                        self.error = str(e)
                        await asyncio.sleep(60 - watch_duration)
                except Exception as e:
                    logger.exception(f"{self} exception")
                    watch_duration = (datetime.now(timezone.utc) - watch_start_dt).total_seconds()
                    if watch_duration < 60:
                        self.state = 'Error'
                        self.error = str(e)
                        await asyncio.sleep(60 - watch_duration)
                self.state = 'Restarting'
                logger.info(f"Restarting {self}")

        except asyncio.CancelledError:
            return

    async def __watch(self, method, **kwargs):
        watch = None
        try:
            watch = kubernetes_asyncio.watch.Watch()
            async for event in watch.stream(method, **kwargs):
                if not isinstance(event, Mapping):
                    raise ResourceWatchFailedError(f"UNKNOWN EVENT: {event}")
                self.error = None
                event_obj = event['object']
                event_type = event['type']
                if not isinstance(event_obj, Mapping):
                    event_obj = self.source().api_client.sanitize_for_serialization(event_obj)
                if event_type == 'ERROR':
                    if event_obj['kind'] == 'Status':
                        if event_obj['reason'] in ('Expired', 'Gone'):
                            raise ResourceWatchRestartError(event_obj['reason'].lower())
                        else:
                            raise ResourceWatchFailedError(f"{event_obj['reason']} {event_obj['message']}")
                    else:
                        raise ResourceWatchFailedError(f"UNKNOWN EVENT: {event}")

                name = event_obj['metadata']['name']
                await self.source().handle_resource_event(event_type=event_type, event_obj=event_obj)
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            if exception.status == 410:
                raise ResourceWatchRestartError("Received 410 expired response.")
            else:
                raise
        finally:
            if watch:
                await watch.close()
