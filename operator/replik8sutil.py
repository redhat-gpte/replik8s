import aiofiles.os
import os

async_utime = aiofiles.os.wrap(os.utime)

async def makedirs_as_needed(path):
    '''
    Make directory and any required parent directories
    '''
    try:
        await aiofiles.os.makedirs(path)
    except FileExistsError:
        pass

async def removedirs_if_empty(path):
    '''
    Make directory and any required parent directories
    '''
    try:
        await aiofiles.os.removedirs(path)
    except (FileNotFoundError, OSError):
        pass
