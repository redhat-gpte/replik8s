import asyncio
import filelock
import json
import kopf
import logging
import os
import pytimeparse
import re
import stat
import time
import weakref

from datetime import datetime, timezone
from typing import Mapping, Optional

import aiofiles.os
import inflection
import kubernetes_asyncio

from aioshutil import rmtree
from base64 import b64decode
from tempfile import mkdtemp

from cachedkopfobject import CachedKopfObject
from replik8s import Replik8s
from replik8sresourcewatch import Replik8sResourceWatch
from replik8sutil import async_utime, makedirs_as_needed, removedirs_if_empty

logger = logging.getLogger('replication_source')

# Disable noisy filelock logger
filelock._logger = logging.getLogger('filelock')
filelock._logger.propagate = False

class Replik8sReplicationSource(CachedKopfObject):
    api_group = Replik8s.api_group
    api_version = Replik8s.api_version
    kind = 'ReplicationSource'
    plural = 'replicationsources'

    cache = {}
    class_lock = asyncio.Lock()
    api_lookup_cache = {}

    @classmethod
    async def on_cleanup(self):
        for replication_source in self.cache.values():
            await replication_source.cleanup()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.watches = []
        self.temp_dir = mkdtemp(prefix='replik8s-source-')

    @property
    def base_dir(self):
        return os.path.join(Replik8s.data_dir, self.namespace, self.name)

    @property
    def cache_dir(self):
        return os.path.join(self.base_dir, 'cache')

    @property
    def have_kubeconfig(self):
        return True if self.spec.get('kubeConfig') else False

    @property
    def kubeconfig_path(self):
        return os.path.join(self.temp_dir, "kubeconfig.yaml")

    @property
    def kubeconfig_secret(self):
        return self.spec.get('kubeConfig', {}).get('secret')

    @property
    def latest_dir(self):
        return os.path.join(self.base_dir, 'latest')

    @property
    def recovery_points_dir(self):
        return os.path.join(self.base_dir, 'recovery-points')

    @property
    def replicate_local(self):
        return self.spec.get('replicateLocal', False)

    @property
    def recovery_point_interval(self):
        value = self.spec.get('recoveryPointInterval', os.environ.get('RECOVERY_POINT_INTERVAL', '10m'))
        if value.isnumeric():
            return int(value)
        return pytimeparse.parse(value)

    @property
    def recovery_point_max_age(self):
        value = self.spec.get('recoveryPointMaxAge', os.environ.get('RECOVERY_POINT_MAX_AGE', '1d'))
        if value.isnumeric():
            return int(value)
        return pytimeparse.parse(value)

    @property
    def refresh_interval(self):
        value = self.spec.get('refreshInterval', os.environ.get('REFRESH_INTERVAL', '10m'))
        if value.isnumeric():
            return int(value)
        return pytimeparse.parse(value)

    @property
    def resources(self):
        return self.spec.get('resources', [])

    def sanity_check(self):
        if self.have_kubeconfig and self.replicate_local:
            raise kopf.TemporaryError(
                f"spec.kubeconfig and spec.replicateLocal are mutually exclusive",
                delay=600
            )
        if not self.have_kubeconfig and not self.replicate_local:
            raise kopf.TemporaryError(
                f"One of spec.kubeconfig or spec.replicateLocal is required",
                delay=600
            )

    async def clean_cache(self):
        '''
        Remove old data from cache
        '''
        try:
            for namespace_or_cluster in await aiofiles.os.listdir(self.cache_dir):
                if namespace_or_cluster.startswith('.'):
                    continue
                try:
                    for plural_with_group in await aiofiles.os.listdir(
                        os.path.join(self.cache_dir, namespace_or_cluster)
                    ):
                        if plural_with_group.startswith('.'):
                            continue
                        cache_subdir = os.path.join(self.cache_dir, namespace_or_cluster, plural_with_group)
                        try:
                            for filename in await aiofiles.os.listdir(cache_subdir):
                                if filename.startswith('.'):
                                    continue
                                filepath = os.path.join(cache_subdir, filename)
                                fstat = await aiofiles.os.stat(filepath)
                                if stat.S_ISREG(fstat.st_mode) \
                                and time.time() > fstat.st_mtime + self.refresh_interval:
                                    logger.debug('removing cache file %s', filepath)
                                    await aiofiles.os.unlink(filepath)
                        except NotADirectoryError:
                            continue
                        await removedirs_if_empty(cache_subdir)
                except NotADirectoryError:
                    pass
        except FileNotFoundError:
            pass

    async def clean_cache(self, logger):
        '''
        Remove old data from cache
        '''
        try:
            logger.debug(f"Clean cache for {self}")
            for namespace_or_cluster in await aiofiles.os.listdir(self.cache_dir):
                if namespace_or_cluster.startswith('.'):
                    continue
                try:
                    for plural_with_group in await aiofiles.os.listdir(
                        os.path.join(self.cache_dir, namespace_or_cluster)
                    ):
                        if plural_with_group.startswith('.'):
                            continue
                        cache_subdir = os.path.join(self.cache_dir, namespace_or_cluster, plural_with_group)
                        try:
                            for filename in await aiofiles.os.listdir(cache_subdir):
                                if filename.startswith('.'):
                                    continue
                                filepath = os.path.join(cache_subdir, filename)
                                fstat = await aiofiles.os.stat(filepath)
                                if stat.S_ISREG(fstat.st_mode) \
                                and time.time() > fstat.st_mtime + 2 * self.refresh_interval:
                                    logger.debug('removing cache file %s', filepath)
                                    await aiofiles.os.unlink(filepath)
                        except NotADirectoryError:
                            continue
                        await removedirs_if_empty(cache_subdir)
                except NotADirectoryError:
                    pass
        except FileNotFoundError:
            pass

    async def clean_latest(self, logger):
        '''
        Remove old data from latest
        '''
        try:
            logger.debug(f"Clean latest for {self}")
            for namespace_or_cluster in await aiofiles.os.listdir(self.latest_dir):
                if namespace_or_cluster.startswith('.'):
                    continue
                try:
                    for plural_with_group in await aiofiles.os.listdir(os.path.join(
                        self.latest_dir, namespace_or_cluster
                    )):
                        if plural_with_group.startswith('.'):
                            continue
                        latest_subdir = os.path.join(self.latest_dir, namespace_or_cluster, plural_with_group)
                        try:
                            for filename in await aiofiles.os.listdir(latest_subdir):
                                if filename.startswith('.') \
                                or not filename.endswith('.json'):
                                    continue
                                filepath = os.path.join(latest_subdir, filename)
                                # No async file lock capability!
                                lock_path = filepath + '.lock'
                                lock = filelock.FileLock(lock_path)
                                with lock:
                                    fstat = os.stat(filepath)
                                    if time.time() > fstat.st_mtime + 2 * self.refresh_interval:
                                        logger.warning(f"Removing orphaned resource json {filepath}")
                                        os.unlink(filepath)
                                    os.unlink(lock_path)
                        except NotADirectoryError:
                            pass
                        await removedirs_if_empty(latest_subdir)
                except NotADirectoryError:
                    pass
        except FileNotFoundError:
            pass

    async def cleanup(self):
        try:
            await rmtree(self.temp_dir)
        except FileNotFoundError:
            pass

    async def copy_latest_to_dir(self, target_dir, logger):
        '''
        Copy all files, using hard links, from latest to a target directory.
        '''
        try:
            for namespace_or_cluster in await aiofiles.os.listdir(self.latest_dir):
                if namespace_or_cluster.startswith('.'):
                    continue
                try:
                    for plural_with_group in await aiofiles.os.listdir(
                        os.path.join(self.latest_dir, namespace_or_cluster)
                    ):
                        if plural_with_group.startswith('.'):
                            continue
                        src_dir = os.path.join(self.latest_dir, namespace_or_cluster, plural_with_group)
                        dst_dir = os.path.join(target_dir, namespace_or_cluster, plural_with_group)

                        await makedirs_as_needed(dst_dir)

                        try:
                            for filename in os.listdir(src_dir):
                                if filename.endswith('.json'):
                                    try:
                                        os.link(
                                            os.path.join(src_dir, filename),
                                            os.path.join(dst_dir, filename)
                                        )
                                    except FileNotFoundError:
                                        pass
                        except NotADirectoryError:
                            pass
                except NotADirectoryError:
                    pass
        except (FileNotFoundError, NotADirectoryError):
            pass

    async def get_recovery_points(self):
        try:
            ret = []
            for item in await aiofiles.os.listdir(self.recovery_points_dir):
                if re.match(r'\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$', item):
                    ret.append(item)
            ret.sort()
            return ret
        except FileNotFoundError:
            return []

    async def handle_delete(self, logger):
        async with self.lock:
            await self.stop_resource_watches(logger=logger)
            try:
                await rmtree(self.base_dir)
            except FileNotFoundError:
                pass
            await self.cleanup()

    async def handle_resource_event(self,
        event_obj: Mapping,
        event_type: Optional[str],
    ):
        resource_api_version = event_obj['apiVersion']
        resource_kind = event_obj['kind']
        resource_metadata = event_obj['metadata']
        resource_name = resource_metadata['name']
        resource_namespace = resource_metadata.get('namespace')
        resource_uid = resource_metadata['uid']
        resource_version = resource_metadata['resourceVersion']
        plural = await self.kind_to_plural(api_version=resource_api_version, kind=resource_kind)
        resource_api_group = resource_api_version.split('/')[0] if '/' in resource_api_version else None
        resource_cache_dir = os.path.join(
            self.cache_dir,
            resource_namespace or '_cluster_',
            plural + '.' + resource_api_group if resource_api_group else plural,
        )
        resource_cache_filepath = os.path.join(
            resource_cache_dir,
            ':'.join((resource_name, resource_uid, resource_version))
        )
        resource_latest_dir = os.path.join(
            self.latest_dir,
            resource_namespace or '_cluster_',
            plural + '.' + resource_api_group if resource_api_group else plural,
        )
        resource_latest_filepath = os.path.join(
            resource_latest_dir,
            resource_name + '.json'
        )

        logger.info(
            f"{event_type} {resource_api_version} {resource_kind} "
            f"{resource_namespace + '/' if resource_namespace else ''}{resource_name} "
            f"({resource_uid}) - {resource_version}"
        )

        if event_type == 'DELETED':
            await self.remove_resource_from_latest(
                resource_latest_dir=resource_latest_dir,
                resource_latest_filepath=resource_latest_filepath
            )
        else:
            await self.write_resource(
                resource=event_obj,
                resource_cache_dir=resource_cache_dir,
                resource_cache_filepath=resource_cache_filepath,
                resource_latest_dir=resource_latest_dir,
                resource_latest_filepath=resource_latest_filepath
            )

    async def init_k8s_client(self,
        logger: kopf.ObjectLogger,
    ):
        if self.replicate_local:
            self.api_client = Replik8s.api_client
            self.core_v1_api = Replik8s.core_v1_api
            self.custom_objects_api = Replik8s.custom_objects_api
        else:
            await self.write_kubeconfig()
            self.api_client = await kubernetes_asyncio.config.new_client_from_config(
                config_file = self.kubeconfig_path
            )
            self.core_v1_api = kubernetes_asyncio.client.CoreV1Api(self.api_client)
            self.custom_objects_api = kubernetes_asyncio.client.CustomObjectsApi(self.api_client)

    async def kind_to_plural(self,
        api_version: str,
        kind: str,
    ):
        if '/' not in self.api_version:
            return inflection.pluralize(kind).lower()
        api = self.api_lookup_cache.get(api_version)
        if api:
            for resource in api['resources']:
                if resource['kind'] == kind:
                    return resource['name']
        if not api:
            try:
                resp = await self.api_client.call_api(
                    method = 'GET',
                    resource_path = f"/apis/{api_version}",
                    auth_settings=['BearerToken'],
                    response_types_map = {
                        200: "object",
                    }
                )
                api = resp[0]
                self.api_lookup_cache[api_version] = api
            except kubernetes_asyncio.client.exceptions.ApiException as exception:
                if exception.status == 404:
                    raise kopf.TemporaryError(
                        f"API {api_version} not found",
                        delay=600
                    )
                else:
                    raise

        for resource in api['resources']:
            if resource['kind'] == kind:
                return resource['name']

    async def make_recovery_point(self, logger):
        '''
        Copy current state of latest directory to make a recovery point.

        Recovery points are made without locking to increase performance at potential
        loss of consistency. An underlying assumption is that kubernetes resource
        definitions should minimally depend on other resource definitions and so
        normally a recovery point not being a consistent point in time is acceptible.
        Anyway, there is no guarantee that watches will strictly deliver resources in
        a strictly chronological order.
        '''
        try:
            recovery_point = datetime.now(timezone.utc).strftime('%FT%TZ')
            logger.info(f"Making recovery point {recovery_point} for {self}")
            recovery_point_dir = os.path.join(self.recovery_points_dir, recovery_point)
            await self.copy_latest_to_dir(target_dir=recovery_point_dir, logger=logger)
        except Exception as e:
            logger.exception('Error while making recovery point!')

    async def prune_recovery_points(self, logger):
        '''
        Remove recovery points based on age
        '''
        try:
            status_recovery_points = []
            recovery_points = await self.get_recovery_points()
            for recovery_point in recovery_points:
                recovery_point_dt = datetime.strptime(recovery_point, '%Y-%m-%dT%H:%M:%S%z')
                recovery_point_age = (datetime.now(timezone.utc) - recovery_point_dt).total_seconds()
                recovery_point_dir = os.path.join(self.recovery_points_dir, recovery_point)
                if recovery_point_age < self.recovery_point_max_age:
                    status_recovery_points.append({
                        "path": recovery_point_dir,
                        "timestamp": recovery_point,
                    })
                else:
                    logger.info(f"Removing recovery point {recovery_point} for {self}")
                    await rmtree(recovery_point_dir)
            await self.json_patch_status([{
                "op": "add",
                "path": "/status/recoveryPoints",
                "value": status_recovery_points,
            }])
        except FileNotFoundError:
            pass

    async def refresh(self, logger):
        for watch in self.watches:
            logger.info(f"Refreshing {self} {watch}")
            await watch.refresh()

    async def remove_resource_from_latest(self, resource_latest_dir, resource_latest_filepath):
        '''
        Remove resource file from latest, if found.
        '''
        try:
            await aiofiles.os.unlink(resource_latest_filepath)
        except FileNotFoundError:
            pass
        await removedirs_if_empty(resource_latest_dir)

    async def start_resource_watches(self,
        logger: kopf.ObjectLogger,
    ):
        await self.stop_resource_watches(logger=logger)
        logger.info(f"Starting resource watches for {self}")
        for resource in self.resources:
            api_version = resource['apiVersion']
            kind = resource['kind']
            plural = await self.kind_to_plural(api_version=api_version, kind=kind)
            watch = Replik8sResourceWatch(
                api_version=resource['apiVersion'],
                kind=kind,
                namespace=resource.get('namespace'),
                plural=plural,
                source=weakref.ref(self),
            )
            self.watches.append(watch)
            await watch.start(logger=logger)

    async def stop_resource_watches(self,
        logger: kopf.ObjectLogger,
    ):
        logger.info(f"Stopping resource watches for {self}")
        for watch in self.watches:
            await watch.stop()
        self.watches = []

    async def update_status(self, logger):
        if self.status.get('latestPath') != self.base_dir:
            logger.info(f"Set status.latestPath for {self} to {self.latest_dir}")
            await self.merge_patch_status({
                "latestPath": self.latest_dir
            })

    async def update_watch_status(self):
        watch_status = []
        for watch in self.watches:
            watch_status_item = {
                "apiVersion": watch.api_version,
                "kind": watch.kind,
                "state": watch.state,
            }
            if watch.error:
                watch_status_item['error'] = watch.error
            if watch.namespace:
                watch_status_item['namespace'] = watch.namespace
            watch_status.append(watch_status_item)
        try:
            if self.status.get('watches') != watch_status:
                await self.json_patch_status([{
                    "op": "add",
                    "path": "/status/watches",
                    "value": watch_status,
                }])
        except kubernetes_asyncio.client.exceptions.ApiException as e:
            # Conflict updating status just means it will be picked up in another pass
            if e.status != 409:
                raise

    async def write_kubeconfig(self):
        secret = await Replik8s.core_v1_api.read_namespaced_secret(
            self.kubeconfig_secret,
            Replik8s.namespace,
        )
        if 'kubeconfig.yaml' not in secret.data:
            raise kopf.TemporaryError(
                f"No kubeconfig.yaml not found in secret "
                f"{secret.metadata.name} in {secret.metadata.namespace}"
            )
        with open(self.kubeconfig_path, 'wb') as fh:
            fh.write(b64decode(secret.data['kubeconfig.yaml']))

    async def write_resource(self,
        resource: str,
        resource_cache_dir: str,
        resource_cache_filepath: str,
        resource_latest_dir: str,
        resource_latest_filepath: str,
    ):
        '''
        Write resource to cache and latest.
        '''
        resource_tmp_filepath = resource_cache_filepath + '.tmp'

        cache_file_exists = False
        try:
            cache_stat = await aiofiles.os.stat(resource_cache_filepath)
            cache_file_exists = True

            # Update timestamp on cache file to track presence
            await async_utime(resource_cache_filepath)

            # Nothing else to do if cache file for version exists and inode
            # matches latest file.
            latest_stat = await aiofiles.os.stat(resource_latest_filepath)
            if cache_stat.st_ino == latest_stat.st_ino:
                return
        except FileNotFoundError:
            pass

        if not cache_file_exists:
            await makedirs_as_needed(resource_cache_dir)
            # Write then rename to guarantee atomic operation
            with open(resource_tmp_filepath, 'w') as f:
                f.write(json.dumps(resource))
                await aiofiles.os.rename(resource_tmp_filepath, resource_cache_filepath)

        await makedirs_as_needed(resource_latest_dir)

        # Create hard-link in latest to cache in two steps because link does
        # not support overwrite.
        try:
            await aiofiles.os.link(resource_cache_filepath, resource_tmp_filepath)
        except FileExistsError:
            # If temporary link location then must have been interrupted,
            # remove and retry.
            await aiofiles.os.unlink(resource_tmp_filepath)
            await aiofiles.os.link(resource_cache_filepath, resource_tmp_filepath)

        # No async file lock capability!
        lock_path = resource_latest_filepath + '.lock'
        lock = filelock.FileLock(lock_path)
        with lock:
            os.rename(resource_tmp_filepath, resource_latest_filepath)
            os.unlink(lock_path)
