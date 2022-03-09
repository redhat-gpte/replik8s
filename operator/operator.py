#!/usr/bin/env python

import asyncio
import atexit
import filelock
import flask
import gevent.pywsgi
import inflection
import json
import kopf
import kubernetes
import logging
import os
import stat
import threading
import time
import yaml

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from base64 import b64decode
from datetime import datetime
from shutil import rmtree
from tempfile import mkdtemp

# Disable noisy filelock logger
filelock._logger = logging.getLogger('filelock')
filelock._logger.propagate = False

datadir = os.environ.get('DATA_DIR', mkdtemp(prefix='data'))
operator_domain = os.environ.get('OPERATOR_DOMAIN', 'replik8s.gpte.redhat.com')
replik8s_source_label = operator_domain + '/source'
# Interval to index recovery point and prune resource data (default 15 minutes)
default_recovery_point_interval = int(os.environ.get('RECOVERY_POINT_INTERVAL', 15 * 60))
# Maximum age for recovery point retention (default 12 hours)
default_recovery_point_max_age = int(os.environ.get('RECOVERY_POINT_MAX_AGE', 12 * 60 * 60))
# Frequency to reset watch to refresh all resources
default_refresh_interval = int(os.environ.get('REFRESH_INTERVAL', 15 * 60))
# Interval for finding and removing source data when source has been removed
source_cleanup_interval = int(os.environ.get('SOURCE_CLEANUP_INTERVAL', 15 * 60))

# API server for client access
api = flask.Flask('rest')

# Temporary directory for working data
tempdir = mkdtemp()
def remove_tempdir():
    rmtree(tempdir)
atexit.register(remove_tempdir)

if os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount'):
    kubernetes.config.load_incluster_config()
else:
    kubernetes.config.load_kube_config()

core_v1_api = kubernetes.client.CoreV1Api()
custom_objects_api = kubernetes.client.CustomObjectsApi()

def makedirs_as_needed(path):
    '''
    Make directory and any required parent directories
    '''
    try:
        os.makedirs(path)
    except FileExistsError:
        pass

def removedirs_if_empty(path):
    '''
    Make directory and any required parent directories
    '''
    try:
        os.removedirs(path)
    except (FileNotFoundError, OSError):
        pass

def run_api():
    http_server = gevent.pywsgi.WSGIServer(('', 5000), api)
    http_server.serve_forever()

def touch(path):
    '''
    Update timestamp on file or create empty file at path
    '''
    try:
        os.utime(path)
    except FileNotFoundError:
        with open(path, 'a') as f:
            pass

class InfiniteRelativeBackoff:
    def __init__(self, initial_delay=0.1, scaling_factor=2, maximum=60):
        self.initial_delay = initial_delay
        self.scaling_factor = scaling_factor
        self.maximum = maximum

    def __iter__(self):
        delay = self.initial_delay
        while True:
            if delay > self.maximum:
                yield self.maximum
            else:
                yield delay
                delay *= self.scaling_factor

class Replik8sConfigError(Exception):
    pass

class Replik8sWatchException(Exception):
    pass

class Replik8sResourceWatch:
    def __init__(self, source, resource):
        self.source = source
        self.resource = resource
        api_version = resource.get('apiVersion', None)
        if api_version and '/' in api_version:
            (self.api_group, self.api_version) = api_version.split('/')
        else:
            (self.api_group, self.api_version) = (None, api_version)
        self.namespace = resource.get('namespace', None)
        self.kind = resource.get('kind', None)
        self.plural = None
        self.healthy = None
        self.error = None

    def __init_core_resource_watcher(self):
        try:
            if self.namespace:
                self.watch_method = getattr(
                    self.source.core_v1_api,
                    'list_namespaced_' + inflection.underscore(self.kind)
                )
                self.watch_method_args = (self.namespace,)
            else:
                self.watch_method = getattr(
                    self.source.core_v1_api,
                    'list_' + inflection.underscore(self.kind)
                )
                self.watch_method_args = ()
        except AttributeError as e:
            raise Replik8sConfigError('Unable to determine watch method. Check apiVersion, plural, and kind are set')

    def __init_custom_resource_watcher(self):
        if self.namespace:
            self.watch_method = self.source.custom_objects_api.list_namespaced_custom_object
            self.watch_method_args = (self.api_group, self.api_version, self.namespace, self.plural)
        else:
            self.watch_method = self.source.custom_objects_api.list_cluster_custom_object
            self.watch_method_args = (self.api_group, self.api_version, self.plural)

    def __sanity_check(self):
        if not self.kind:
            raise Replik8sConfigError('Source resources must define kind')
        if not self.api_version:
            raise Replik8sConfigError('Source resources must define apiVersion')

    @property
    def name(self):
        if self.namespace and self.api_group:
            return os.path.join(self.namespace, self.plural + '.' + self.api_group)
        elif self.namespace:
            return os.path.join(self.namespace, self.plural)
        elif self.api_group:
            return self.plural + '.' + self.api_group
        else:
            return self.plural

    def handle_resource_event(self, event):
        event_type = event['type']
        resource = event['object']
        if hasattr(resource, 'to_dict'):
            resource = resource.to_dict()

        resource_kind = resource['kind']
        resource_metadata = resource['metadata']
        resource_name = resource_metadata['name']
        resource_namespace = resource_metadata.get('namespace')
        resource_uid = resource_metadata['uid']
        resource_version = resource_metadata['resourceVersion']

        resource_cache_dir = os.path.join(
            self.source.cache_dir,
            resource_namespace or '_cluster_',
            self.plural + '.' + self.api_group if self.api_group else self.plural,
        )
        resource_cache_filepath = os.path.join(
            resource_cache_dir,
            ':'.join((resource_name, resource_uid, resource_version))
        )
        resource_latest_dir = os.path.join(
            self.source.latest_dir,
            resource_namespace or '_cluster_',
            self.plural + '.' + self.api_group if self.api_group else self.plural,
        )
        resource_latest_filepath = os.path.join(
            resource_latest_dir,
            resource_name + '.json'
        )

        self.source.logger.info('%s %s %s (%s) version %s',
            event_type, resource_kind,
            '{}/{}'.format(resource_namespace, resource_name) if resource_namespace else resource_name,
            resource_uid, resource_version
        )

        if event_type == 'DELETED':
            self.remove_resource_from_latest(
                resource_latest_dir=resource_latest_dir,
                resource_latest_filepath=resource_latest_filepath
            )
        else:
            self.write_resource(
                resource=resource,
                resource_cache_dir=resource_cache_dir,
                resource_cache_filepath=resource_cache_filepath,
                resource_latest_dir=resource_latest_dir,
                resource_latest_filepath=resource_latest_filepath
            )

    def refresh(self):
        self.source.logger.info('refreshing %s/%s %s', self.source.namespace, self.source.name, self.name)
        for resource in self.watch_method(*self.watch_method_args).get('items', []):
            self.handle_resource_event({
                'type': 'REFRESH',
                'object': resource,
            })

    def remove_resource_from_latest(self, resource_latest_dir, resource_latest_filepath):
        '''
        Remove resource file from latest, if found.
        '''
        try:
            os.unlink(resource_latest_filepath)
        except FileNotFoundError:
            pass
        removedirs_if_empty(resource_latest_dir)

    def start(self):
        try:
            self.__sanity_check()
            self.plural = self.source.resource_kind_to_plural(self.api_group, self.api_version, self.kind)
            if self.api_group:
                self.__init_custom_resource_watcher()
            else:
                self.__init_core_resource_watcher()
        except Exception as e:
            self.source.logger.error('Config error in resources: %s', e)
            self.healthy = False
            self.error = str(e)
            return

        self.source.logger.info(
            "Start resource watch %s/%s for %s", self.source.namespace, self.source.name, self.name
        )

        self.stop = False
        self.thread = threading.Thread(
            name = os.path.join(self.source.name, self.name),
            target = self.watch_loop
        )
        self.thread.daemon = True
        self.thread.start()

    def watch(self):
        stream = kubernetes.watch.Watch().stream(self.watch_method, *self.watch_method_args)
        watch_start_time = time.time()

        # Update modification timestamp on watch touchpoint
        touch(self.source.watch_activity_touchpoint)

        for event in stream:
            if self.stop:
                return

            # Handle event object
            event_obj = event['object']
            if event['type'] == 'ERROR' \
            and event_obj['kind'] == 'Status':
                self.source.logger.debug('Watch %s - reason %s, %s',
                    event_obj['status'],
                    event_obj['reason'],
                    event_obj['message']
                )
                if event_obj['status'] == 'Failure':
                    if event_obj['reason'] in ('Expired', 'Gone'):
                        self.source.logger.info(
                            'Restarting watch %s, reason %s',
                            self.watch_method_args, event_obj['reason']
                        )
                        return
                    else:
                        raise Replik8sWatchException(
                            "Watch failure: reason {}, message {}".format(
                                event_obj['reason'], event_obj['message']
                            )
                        )
            else:
                try:
                    self.healthy = True
                    self.handle_resource_event(event)
                except Exception as e:
                    self.source.logger.exception("Error handling event")

    def watch_loop(self):
        while True:
            try:
                self.watch()
            except kubernetes.client.rest.ApiException as e:
                if e.status == 410:
                    self.source.logger.debug("Watch expired, restarting")
                else:
                    self.error = str(e)
                    self.healthy = False
                    self.source.logger.exception("API error on watch")
                    time.sleep(60)
            except Exception as e:
                self.error = str(e)
                self.healthy = False
                self.source.logger.exception("Error in watch loop")
                time.sleep(30)

    def write_resource(
        self, resource,
        resource_cache_dir, resource_cache_filepath,
        resource_latest_dir, resource_latest_filepath
    ):
        '''
        Write resource to cache and latest.
        '''
        resource_tmp_filepath = resource_cache_filepath + '.tmp'

        cache_file_exists = False
        try:
            cache_stat = os.stat(resource_cache_filepath)
            cache_file_exists = True

            # Update timestamp on cache file to track presence
            os.utime(resource_cache_filepath)

            # Nothing else to do if cache file for version exists and inode
            # matches latest file.
            latest_stat = os.stat(resource_latest_filepath)
            if cache_stat.st_ino == latest_stat.st_ino:
                return
        except FileNotFoundError:
            pass

        if not cache_file_exists:
            makedirs_as_needed(resource_cache_dir)
            # Write then rename to guarantee atomic operation
            with open(resource_tmp_filepath, 'w') as f:
                f.write(json.dumps(resource))
                os.rename(resource_tmp_filepath, resource_cache_filepath)

        makedirs_as_needed(resource_latest_dir)

        # Create hard-link in latest to cache in two steps because link does
        # not support overwrite.
        try:
            os.link(resource_cache_filepath, resource_tmp_filepath)
        except FileExistsError:
            # If temporary link location then must have been interrupted,
            # remove and retry.
            os.unlink(resource_tmp_filepath)
            os.link(resource_cache_filepath, resource_tmp_filepath)

        lock = filelock.FileLock(resource_latest_filepath + '.lock')
        with lock:
            os.rename(resource_tmp_filepath, resource_latest_filepath)

class Replik8sSource:
    # List of Replik8sSources
    sources = {}

    # Source lock used to control sources changing during recovery point processing
    source_lock = threading.Lock()

    @classmethod
    def get(cls, namespace=None, name=None):
        '''
        Get source by metadata or name + namespace
        '''
        if metadata:
            namespace = metadata['namespace']
            name = metadata['name']
        key = (namespace, name)
        return cls.sources.get(key, None)

    @classmethod
    def get_by_token(cls, token):
        '''
        Get sources matching auth token
        '''
        return [ source for source in cls.sources.values() if source.match_token(token) ]

    @classmethod
    def load_config_map(cls, body, labels, name, namespace, **_):
        key = (namespace, name)
        source = cls.sources.get(key)

        spec_yaml = body.get('data', {}).get('spec')
        if not spec_yaml:
            raise kopf.PermanentError('Source configmap missing spec data')

        try:
            spec = yaml.safe_load(spec_yaml)
        except yaml.parser.ParserError as e:
            raise kopf.PermanentError(f"YAML parse error loading configmap spec data: {e}")

        with cls.source_lock:
            return cls.new_or_update(
                api_version = 'v1',
                kind = 'ConfigMap',
                labels = labels,
                name = name,
                namespace = namespace,
                spec = spec
            )

    @classmethod
    def new_or_update(cls, api_version, kind, labels, name, namespace, spec):
        '''
        Register new source or update currently registered source.
        '''
        key = (namespace, name)
        source = cls.sources.get(key, None)
        if source:
            source.__update(spec=spec)
        else:
            source = cls(
                api_version = api_version,
                kind = kind,
                labels = labels,
                name = name,
                namespace = namespace,
                spec = spec
            )
            cls.sources[key] = source
        return source

    @classmethod
    def remove(cls, name, namespace):
        '''
        Remove source and stop watches for source.
        '''
        key = (namespace, name)
        with cls.source_lock:
            source = cls.sources.get(key, None)
            if source:
                del cls.sources[key]
                source.stop_resources_watch()
                rmtree(self.basedir)

    def __init__(self, api_version, kind, labels, name, namespace, spec):
        self.api_version = api_version
        self.kind = kind
        self.labels = labels
        self.name = name
        self.namespace = namespace
        self.spec = spec

        # Use kopf local object logger to log without attempting to create events
        self.logger = kopf.LocalObjectLogger(
            body = self.to_dict(),
            settings = kopf.OperatorSettings(),
        )

        # Replik8sResourceWatch objects for this source
        self.api_groups = dict()
        self.lock = threading.Lock()
        self.watches = list()
        self.__sanity_check()
        self.__init_kube_api()

    def __init_kube_api(self):
        if self.kube_config_secret:
            self.write_kube_config()
            self.api_client = kubernetes.config.new_client_from_config(config_file=self.kube_config_path)
            self.core_v1_api = kubernetes.client.CoreV1Api(api_client=self.api_client)
            self.custom_objects_api = kubernetes.client.CustomObjectsApi(api_client=self.api_client)
        else:
            self.api_client = core_v1_api.api_client
            self.core_v1_api = core_v1_api
            self.custom_objects_api = custom_objects_api

    def __sanity_check(self):
        if not isinstance(self.resources, list):
            raise kopf.PermanentError('Source must include resources as list')
        if self.kube_config:
            if not isinstance(self.kube_config, dict):
                raise Replik8sConfigError('Source kubeConfig must be a dictionary')
            if not self.kube_config_secret:
                raise Replik8sConfigError('Source kubeConfig.secret is required')
            if not isinstance(self.kube_config_secret, str):
                raise Replik8sConfigError('source kubeConfig.secret must be a string')

    def __update(self, spec):
        self.spec = spec
        self.__sanity_check()

    @property
    def auth(self):
        return self.spec.get('auth', None)

    @property
    def basedir(self):
        return os.path.join(datadir, self.namespace, self.name)

    @property
    def cache_dir(self):
        return os.path.join(self.basedir, 'cache')

    @property
    def latest_dir(self):
        return os.path.join(self.basedir, 'latest')

    @property
    def kube_config(self):
        return self.spec.get('kubeConfig', None)

    @property
    def kube_config_path(self):
        return "{}/{}.kubeconfig".format(tempdir, self.name)

    @property
    def kube_config_secret(self):
        kube_config = self.kube_config
        if kube_config:
            return self.kube_config.get('secret', None)

    @property
    def recovery_points(self):
        try:
            return [
                d for d in os.listdir(self.recovery_points_dir) if not d.startswith('.')
            ]
        except FileNotFoundError:
            return []

    @property
    def recovery_points_dir(self):
        return os.path.join(self.basedir, 'recovery-points')

    @property
    def recovery_point_interval(self):
        return float(self.spec.get('recoveryPointInterval', default_recovery_point_interval))

    @property
    def recovery_point_max_age(self):
        return float(self.spec.get('recoveryPointMaxAge', default_recovery_point_max_age))

    @property
    def refresh_interval(self):
        return float(self.spec.get('refreshInterval', default_refresh_interval))

    @property
    def resources(self):
        return self.spec.get('resources', None)

    @property
    def watch_activity_touchpoint(self):
        return os.path.join(self.basedir, '.watchactive')

    def clean_cache(self, logger):
        '''
        Remove old data from cache
        '''
        try:
            logger.debug('cache clean for %s/%s', self.namespace, self.name)
            for namespace_or_cluster in os.listdir(self.cache_dir):
                if namespace_or_cluster.startswith('.'):
                    continue
                try:
                    for plural_with_group in os.listdir(os.path.join(
                        self.cache_dir, namespace_or_cluster
                    )):
                        if plural_with_group.startswith('.'):
                            continue
                        cache_subdir = os.path.join(self.cache_dir, namespace_or_cluster, plural_with_group)
                        try:
                            for filename in os.listdir(cache_subdir):
                                if filename.startswith('.'):
                                    continue
                                filepath = os.path.join(cache_subdir, filename)
                                fstat = os.stat(filepath)
                                if stat.S_ISREG(fstat.st_mode) \
                                and time.time() > fstat.st_mtime + self.refresh_interval:
                                    logger.debug('removing cache file %s', filepath)
                                    os.unlink(filepath)
                        except NotADirectoryError:
                            continue
                        removedirs_if_empty(cache_subdir)
                except NotADirectoryError:
                    pass
        except FileNotFoundError:
            pass

    def clean_latest(self, logger):
        '''
        Remove old data from latest
        '''
        try:
            logger.debug('clean of latest for %s/%s', self.namespace, self.name)
            for namespace_or_cluster in os.listdir(self.latest_dir):
                if namespace_or_cluster.startswith('.'):
                    continue
                try:
                    for plural_with_group in os.listdir(os.path.join(
                        self.latest_dir, namespace_or_cluster
                    )):
                        if plural_with_group.startswith('.'):
                            continue
                        latest_subdir = os.path.join(self.latest_dir, namespace_or_cluster, plural_with_group)
                        try:
                            for filename in os.listdir(latest_subdir):
                                if filename.startswith('.') \
                                or not filename.endswith('.json'):
                                    continue
                                filepath = os.path.join(latest_subdir, filename)
                                lock = filelock.FileLock(filepath + '.lock')
                                with lock:
                                    fstat = os.stat(filepath)
                                    if time.time() > fstat.st_mtime + 2 * self.refresh_interval:
                                        logger.warning('removing orphaned resource json %s', filepath)
                                        os.unlink(filepath)
                        except NotADirectoryError:
                            pass
                        removedirs_if_empty(latest_subdir)
                except NotADirectoryError:
                    pass
        except FileNotFoundError:
            pass

    def discover_api_group(self, api_group, version):
        resp = self.api_client.call_api(
            '/apis/{}/{}'.format(api_group,version),
            'GET',
            auth_settings=['BearerToken'],
            response_type='object'
        )
        group_info = resp[0]
        if api_group not in self.api_groups:
            self.api_groups[api_group] = {}
        self.api_groups[api_group][version] = group_info

    def get_items_from_dir(self, directory):
        try:
            items = []
            for namespace_or_cluster in os.listdir(self.latest_dir):
                if namespace_or_cluster.startswith('.'):
                    continue
                try:
                    for plural_with_group in os.listdir(os.path.join(
                        self.latest_dir, namespace_or_cluster
                    )):
                        if plural_with_group.startswith('.'):
                            continue
                        latest_subdir = os.path.join(self.latest_dir, namespace_or_cluster, plural_with_group)
                        try:
                            for filename in os.listdir(latest_subdir):
                                if filename.startswith('.') \
                                or not filename.endswith('.json'):
                                    continue
                                filepath = os.path.join(latest_subdir, filename)
                                lock = filelock.FileLock(filepath + '.lock')
                                with lock:
                                    try:
                                        with open(filepath, 'r') as f:
                                            items.append(json.load(f))
                                    except FileNotFoundError:
                                        pass
                        except NotADirectoryError:
                            pass
                        removedirs_if_empty(latest_subdir)
                except NotADirectoryError:
                    pass
            return items
        except FileNotFoundError:
            return None

    def get_latest_items(self):
        return self.get_items_from_dir(self.latest_dir) or []

    def get_recovery_point_items(self, recovery_point):
        return self.get_items_from_dir(os.path.join(self.recovery_point_dir, recovery_point))

    def prune_recovery_points(self, logger):
        '''
        Remove recovery points based on age
        '''
        try:
            for recovery_point_dir in os.listdir(self.recovery_points_dir):
                if recovery_point_dir.startswith('.'):
                    continue
                dstat = os.stat(recovery_point_dir)
                if not stat.S_ISDIR(dstat.st_mode):
                    continue
                if time.time() > dstat.st_mtime + self.recovery_point_max_age:
                    rmtree(recovery_point_dir)
        except FileNotFoundError:
            pass

    def copy_latest_to_dir(self, target_dir, logger):
        '''
        Copy all files, using hard links, from latest to a target directory.
        '''
        try:
            for namespace_or_cluster in os.listdir(self.latest_dir):
                if namespace_or_cluster.startswith('.'):
                    continue
                try:
                    for plural_with_group in os.listdir(os.path.join(
                        self.latest_dir, namespace_or_cluster
                    )):
                        if plural_with_group.startswith('.'):
                            continue
                        src_dir = os.path.join(self.latest_dir, namespace_or_cluster, plural_with_group)
                        dst_dir = os.path.join(target_dir, namespace_or_cluster, plural_with_group)

                        makedirs_as_needed(dst_dir)

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
        except NotADirectoryError:
            pass

    def make_recovery_point(self, logger):
        '''
        Copy current state of latest directory to make a recovery point.

        Recovery points are made without locking to increase performance at potential
        loss of consistency. An underlying assumption is that kubernetes resource
        definitions should minimally depend on other resource definitions and so
        normally a recovery point not being a consistent point in time is acceptible.
        '''
        # FIXME? - Add optional support locking for recovery points creation?
        try:
            recovery_point = datetime.utcnow().isoformat() + 'Z'
            logger.info(f"Making recovery point {recovery_point}")
            recovery_point_dir = os.path.join(self.recovery_points_dir, recovery_point)
            self.copy_latest_to_dir(target_dir=recovery_point_dir, logger=logger)
        except Exception as e:
            logger.exception('Error while making recovery point!')
        self.update_recovery_point_status()

    def match_token(self, token):
        '''
        Return boolean indicating whether token matches source.
        '''
        if not self.auth:
            return False
        if 'token' in self.auth:
            return token == self.auth['token']
        if 'secret' in self.auth:
            try:
                secret = core_v1_api.read_namespaced_secret(self.auth['secret'], self.namespace)
                if 'token' in secret.data:
                    return token == b64decode(secret.data['token'])
                else:
                    self.logger.warning('secret %s missing token data', self.auth['secret'])
                    return False
            except kubernetes.client.rest.ApiException as e:
                if e.status == 404:
                    self.logger.warning('secret %s not found', self.auth['secret'])
                    return False
                else:
                    raise
        self.logger.warning('no auth method defined for source')
        return False

    def refresh(self):
        self.logger.info('refreshing source')
        for watch in self.watches:
            watch.refresh()

    def resource_kind_to_plural(self, api_group, version, kind):
        if not api_group:
            return inflection.pluralize(kind).lower()

        if api_group not in self.api_groups \
        or version not in self.api_groups[api_group]:
            self.discover_api_group(api_group, version)

        for resource in self.api_groups[api_group][version]['resources']:
            if resource['kind'] == kind:
                return resource['name']
        raise Replik8sConfigError('unable to find kind {} in {}/{}', kind, api_group, version)

    def save_config(self):
        '''
        Save config definition from Kopf object.
        '''
        makedirs_as_needed(self.basedir)
        with open(os.path.join(self.basedir, 'config.yaml'), 'w') as f:
            f.write(f"#{datetime.utcnow().isoformat()}Z\n")
            f.write(yaml.safe_dump(self.to_dict(), default_flow_style=False))

    def start_resource_watch(self, resource):
        watch = Replik8sResourceWatch(source=self, resource=resource)
        self.watches.append(watch)
        watch.start()

    def start_resources_watch(self):
        self.last_refresh_time = time.time()
        if self.watches:
            self.stop_resources_watch()
        self.watches = []
        self.logger.debug('starting resources watch for %s/%s', self.namespace, self.name)
        for resource in self.resources:
            self.start_resource_watch(resource)

    def stop_resources_watch(self):
        self.logger.info('stop resource watches for %s', self.name)
        for watch in self.watches:
            watch.stop = True

    def to_dict(self):
        ret = dict(
            apiVersion = self.api_version,
            kind = self.kind,
            metadata = dict(
                name = self.name,
                namespace = self.namespace,
            )
        )
        if self.labels:
            ret['metadata']['labels'] = {**self.labels}
        if self.kind == 'ConfigMap':
            ret['data'] = dict(spec = json.dumps(self.spec))
        else:
            ret['spec'] = {**self.spec}
        return ret

    def update_recovery_point_status(self):
        try:
            if self.kind == 'ConfigMap':
                config_map = core_v1_api.read_namespaced_config_map(self.name, self.namespace)
                status = yaml.safe_load(config_map.data['status']) if 'status' in config_map.data else {}
                status['recoveryPoints'] = self.recovery_points
                config_map.data['status'] = yaml.safe_dump(status, default_flow_style=False)
                core_v1_api.replace_namespaced_config_map(self.name, self.namespace, config_map)
            else:
                # FIXME? - Future support for custom resource kind?
                pass
        except kubernetes.client.rest.ApiException as e:
            # Conflict updating status just means it will be picked up in another pass
            if e.status != 409:
                raise

    def update_watch_status(self):
        watch_status = []
        for watch in self.watches:
            watch_status_item = {
                "healthy": watch.healthy,
                "kind": watch.kind,
            }
            if watch.api_group:
                watch_status_item['apiVersion'] = f"{watch.api_group}/{watch.api_version}"
            else:
                watch_status_item['apiVersion'] = f"{watch.api_version}"
            if not watch.healthy:
                watch_status_item['error'] = watch.error
            watch_status.append(watch_status_item)
        try:
            if self.kind == 'ConfigMap':
                config_map = core_v1_api.read_namespaced_config_map(self.name, self.namespace)
                status = yaml.safe_load(config_map.data['status']) if 'status' in config_map.data else {}
                if watch_status != status.get('watches'):
                    status['watches'] = watch_status
                    config_map.data['status'] = yaml.safe_dump(status, default_flow_style=False)
                    core_v1_api.replace_namespaced_config_map(self.name, self.namespace, config_map)
            else:
                # FIXME? - Future support for custom resource kind?
                pass
        except kubernetes.client.rest.ApiException as e:
            # Conflict updating status just means it will be picked up in another pass
            if e.status != 409:
                raise

    def write_kube_config(self):
        secret = core_v1_api.read_namespaced_secret(self.kube_config_secret, self.namespace)
        if 'kubeconfig.yaml' not in secret.data:
            raise kopf.TemporaryError('No kubeconfig.yaml not found in secret {} in {}'.format(
                secret.metadata.name, secret.metadata.namespace
            ))
        with open(self.kube_config_path, 'wb') as fh:
            fh.write(b64decode(secret.data['kubeconfig.yaml']))

@kopf.on.startup()
def startup(settings: kopf.OperatorSettings, **_):
    # Never give up from network errors
    settings.networking.error_backoffs = InfiniteRelativeBackoff()

    # Only create events for warnings and errors
    settings.posting.level = logging.WARNING

    # Disable scanning for CustomResourceDefinitions
    settings.scanning.disabled = True

    # Start api
    threading.Thread(
        name = 'api',
        target = run_api,
        daemon = True,
    ).start()


@kopf.on.create('configmaps', labels={replik8s_source_label: kopf.PRESENT})
def config_create(**kwargs):
    source = Replik8sSource.load_config_map(**kwargs)
    source.save_config()
    source.start_resources_watch()

@kopf.on.resume('configmaps', labels={replik8s_source_label: kopf.PRESENT})
def config_resume(**kwargs):
    source = Replik8sSource.load_config_map(**kwargs)
    source.save_config()
    source.start_resources_watch()

@kopf.on.update('configmaps', labels={replik8s_source_label: kopf.PRESENT})
def config_update(diff, **kwargs):
    # Ignore update if spec was not changed
    if 0 == len([x for x in diff if x[1] == ('data', 'spec')]):
        return
    source = Replik8sSource.load_config_map(**kwargs)
    source.save_config()
    source.start_resources_watch()

@kopf.on.delete('configmaps', labels={replik8s_source_label: kopf.PRESENT})
def config_delete(name, namespace, **_):
    Replik8sSource.remove(name=name, namespace=namespace)

@kopf.daemon('configmaps', labels={replik8s_source_label: kopf.PRESENT})
async def config_refresh(stopped, **kwargs):
    source = Replik8sSource.load_config_map(**kwargs)
    try:
        while not stopped:
            await asyncio.sleep(source.refresh_interval)
            with source.lock:
                source.refresh()
    except asyncio.CancelledError:
        pass

@kopf.daemon('configmaps', labels={replik8s_source_label: kopf.PRESENT})
async def config_manage_recovery_points(logger, stopped, **kwargs):
    source = Replik8sSource.load_config_map(**kwargs)
    try:
        while not stopped:
            await asyncio.sleep(source.recovery_point_interval)
            with source.lock:
                source.make_recovery_point(logger=logger)
                source.prune_recovery_points(logger=logger)
                source.clean_cache(logger=logger)
                source.clean_latest(logger=logger)
    except asyncio.CancelledError:
        pass

@kopf.daemon('configmaps', labels={replik8s_source_label: kopf.PRESENT})
async def config_update_watch_status(logger, stopped, **kwargs):
    source = Replik8sSource.load_config_map(**kwargs)
    try:
        while not stopped:
            await asyncio.sleep(10)
            with source.lock:
                source.update_watch_status()
    except asyncio.CancelledError:
        pass

def get_auth_token():
    auth_header = flask.request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        flask.abort(400)
    return auth_header[7:]

@api.route('/sources', methods=['GET'])
def get_sources():
    token = get_auth_token()
    sources = Replik8sSource.get_by_token(token)
    return flask.jsonify([
        source.namespace + '/' + source.name for source in sources
    ])

@api.route('/sources/<string:source_namespace>/<string:source_name>/latest', methods=['GET'])
def get_latest(source_namespace, source_name):
    token = get_auth_token()
    source = Replik8sSource.get(namespace=source_namespace, name=source_name)
    if not source and source.match_token(token):
        flask.abort(400)

    return flask.jsonify(dict(
        apiVersion='v1',
        kind='List',
        metadata=dict(),
        items=source.get_latest_items()
    ))

@api.route('/sources/<string:source_namespace>/<string:source_name>/recovery-points', methods=['GET'])
def get_recovery_points(source_namespace, source_name):
    token = get_auth_token()
    source = Replik8sSource.get(namespace=source_namespace, name=source_name)
    if not source and source.match_token(token):
        flask.abort(400)
    return flask.jsonify(source.recovery_points)

@api.route('/sources/<string:source_namespace>/<string:source_name>/recovery-points/<string:recovery_point>', methods=['GET'])
def get_recovery_point(source_namespace, source_name, recovery_point):
    token = get_auth_token()
    source = Replik8sSource.get(namespace=source_namespace, name=source_name)
    if not source and source.match_token(token):
        flask.abort(400)

    items = source.get_recovery_point_items(recovery_point)
    if items == None:
        flask.abort(404)

    return flask.jsonify(dict(
        apiVersion='v1',
        kind='List',
        metadata=dict(),
        items=items
    ))
