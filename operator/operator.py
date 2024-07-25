#!/usr/bin/env python
import logging

import aiohttp
from aiohttp import web

import asyncio
import kopf

from infinite_relative_backoff import InfiniteRelativeBackoff
from replik8s import Replik8s
from replik8sreplicationsource import Replik8sReplicationSource

@kopf.on.startup()
async def startup(logger: kopf.ObjectLogger, settings: kopf.OperatorSettings, **_):
    # Store last handled configuration in status
    settings.persistence.diffbase_storage = kopf.StatusDiffBaseStorage(field='status.diffBase')

    # Never give up from network errors
    settings.networking.error_backoffs = InfiniteRelativeBackoff()

    # Use operator domain as finalizer
    settings.persistence.finalizer = Replik8s.api_group

    # Store progress in status.
    settings.persistence.progress_storage = kopf.StatusProgressStorage(field='status.kopf.progress')

    # Only create events for warnings and errors
    settings.posting.level = logging.WARNING

    # Disable scanning
    settings.scanning.disabled = True

    # Preload for matching ResourceClaim templates
    await Replik8s.on_startup()

@kopf.on.cleanup()
async def cleanup(logger: kopf.ObjectLogger, **_):
    await Replik8sReplicationSource.on_cleanup()
    await Replik8s.on_cleanup()

@kopf.on.create(
    Replik8sReplicationSource.api_group, Replik8sReplicationSource.api_version, Replik8sReplicationSource.plural,
    id='replication_source_create',
)
@kopf.on.resume(
    Replik8sReplicationSource.api_group, Replik8sReplicationSource.api_version, Replik8sReplicationSource.plural,
    id='replication_source_resume',
)
@kopf.on.update(
    Replik8sReplicationSource.api_group, Replik8sReplicationSource.api_version, Replik8sReplicationSource.plural,
    id='replication_source_update',
)
async def on_replication_source_event(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    uid: str,
    **_
):
    replication_source = await Replik8sReplicationSource.load(
        annotations=annotations,
        labels=labels,
        meta=meta,
        name=name,
        namespace=namespace,
        spec=spec,
        status=status,
        uid=uid,
    )
    replication_source.sanity_check()
    await replication_source.update_status(logger=logger)
    await replication_source.init_k8s_client(logger=logger)
    await replication_source.start_resource_watches(logger=logger)

@kopf.on.delete(
    Replik8sReplicationSource.api_group, Replik8sReplicationSource.api_version, Replik8sReplicationSource.plural,
    id='replication_source_delete',
)
async def on_replication_source_delete(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    uid: str,
    **_
):
    replication_source = await Replik8sReplicationSource.load(
        annotations=annotations,
        labels=labels,
        meta=meta,
        name=name,
        namespace=namespace,
        spec=spec,
        status=status,
        uid=uid,
    )
    await replication_source.handle_delete(logger=logger)

@kopf.daemon(
    Replik8sReplicationSource.api_group, Replik8sReplicationSource.api_version, Replik8sReplicationSource.plural,
    cancellation_timeout = 1,
)
async def replication_source_refresh(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    stopped,
    uid: str,
    **_
):
    replication_source = await Replik8sReplicationSource.load(
        annotations=annotations,
        labels=labels,
        meta=meta,
        name=name,
        namespace=namespace,
        spec=spec,
        status=status,
        uid=uid,
    )
    try:
        while not stopped:
            await asyncio.sleep(replication_source.refresh_interval)
            await replication_source.refresh(logger=logger)
    except asyncio.CancelledError:
        pass

@kopf.daemon(
    Replik8sReplicationSource.api_group, Replik8sReplicationSource.api_version, Replik8sReplicationSource.plural,
    cancellation_timeout = 1,
)
async def replication_source_manage_recovery_points(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    stopped,
    uid: str,
    **_
):
    replication_source = await Replik8sReplicationSource.load(
        annotations=annotations,
        labels=labels,
        meta=meta,
        name=name,
        namespace=namespace,
        spec=spec,
        status=status,
        uid=uid,
    )
    try:
        while not stopped:
            await asyncio.sleep(replication_source.recovery_point_interval)
            async with replication_source.lock:
                logger.info(f"Managing recovery ponits for {replication_source}")
                await replication_source.make_recovery_point(logger=logger)
                await replication_source.prune_recovery_points(logger=logger)
                await replication_source.clean_cache(logger=logger)
                await replication_source.clean_latest(logger=logger)
    except asyncio.CancelledError:
        pass

@kopf.daemon(
    Replik8sReplicationSource.api_group, Replik8sReplicationSource.api_version, Replik8sReplicationSource.plural,
    cancellation_timeout = 1,
)
async def replication_source_update_watch_status(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    stopped,
    uid: str,
    **_
):
    replication_source = await Replik8sReplicationSource.load(
        annotations=annotations,
        labels=labels,
        meta=meta,
        name=name,
        namespace=namespace,
        spec=spec,
        status=status,
        uid=uid,
    )
    try:
        while not stopped:
            await asyncio.sleep(10)
            async with replication_source.lock:
                await replication_source.update_watch_status()
    except asyncio.CancelledError:
        pass
