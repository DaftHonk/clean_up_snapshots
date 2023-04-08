"""
Support for automating the deletion of backups.
"""
import logging
import os

import pytz
from dateutil.parser import parse
import asyncio
import aiohttp
import async_timeout
from urllib.parse import urlparse

from homeassistant.const import (CONF_HOST, CONF_TOKEN)
import homeassistant.helpers.config_validation as cv
import voluptuous as vol

_LOGGER = logging.getLogger(__name__)

DOMAIN = 'clean_backups'
ATTR_NAME = 'number_to_keep'
DEFAULT_NUM = 0
BACKUPS_URL_PATH = 'backups'

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Optional(ATTR_NAME, default=DEFAULT_NUM): int,
    }),
}, extra=vol.ALLOW_EXTRA)

async def async_setup(hass, config):
    conf = config[DOMAIN]
    supervisor_url = 'http://supervisor/'
    auth_token = os.getenv('SUPERVISOR_TOKEN')
    num_to_keep = conf.get(ATTR_NAME, DEFAULT_NUM)
    headers = {'authorization': "Bearer {}".format(auth_token)}

    async def async_get_backups():
        _LOGGER.info('Calling get backups')
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            try:
                with async_timeout.timeout(10):
                    resp = await session.get(supervisor_url + BACKUPS_URL_PATH, headers=headers)
                data = await resp.json()
                await session.close()
                return data['data']['backups']
            except aiohttp.ClientError:
                _LOGGER.error("Client error on calling get backups", exc_info=True)
                await session.close()
            except asyncio.TimeoutError:
                _LOGGER.error("Client timeout error on get backups", exc_info=True)
                await session.close()
            except Exception:
                _LOGGER.error("Unknown exception thrown", exc_info=True)
                await session.close()

    async def async_remove_backups(stale_backups):
        for backup in stale_backups:
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                _LOGGER.info('Attempting to remove backup: slug=%s', backup['slug'])
                # call hassio API deletion
                try:
                    with async_timeout.timeout(10):
                        resp = await session.delete(supervisor_url + f"{BACKUPS_URL_PATH}/" + backup['slug'], headers=headers)
                    res = await resp.json()
                    if res['result'].lower() == "ok":
                        _LOGGER.info("Deleted backup %s", backup["slug"])
                        await session.close()
                        continue
                    else:
                        # log an error
                        _LOGGER.warning("Failed to delete backup %s: %s", backup["slug"], str(res.status_code))

                except aiohttp.ClientError:
                    _LOGGER.error("Client error on calling delete backup", exc_info=True)
                    await session.close()
                except asyncio.TimeoutError:
                    _LOGGER.error("Client timeout error on delete backup", exc_info=True)
                    await session.close()
                except Exception:
                    _LOGGER.error("Unknown exception thrown on calling delete backup", exc_info=True)
                    await session.close()

    async def async_handle_clean_up(call):
        # Allow the service call to override the configuration.
        num_to_keep = call.data.get(ATTR_NAME, num_to_keep)
        _LOGGER.info('Number of backups we are going to keep: %s', str(num_to_keep))

        if num_to_keep == 0:
            _LOGGER.info('Number of backups to keep was zero which is default so no backups will be removed')
            return

        snapshots = await async_get_backups()
        _LOGGER.info('Backups: %s', backups)

        # filter the backups
        if backups is not None:
            for backup in backups:
                d = parse(backup["date"])
                if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
                    _LOGGER.info("Naive DateTime found for backup %s, setting to UTC...", backup["slug"])
                    backup["date"] = d.replace(tzinfo=pytz.utc).isoformat()
            backups.sort(key=lambda item: parse(item["date"]), reverse=True)
            stale_backups = backups[num_to_keep:]
            _LOGGER.info('Stale Backups: {}'.format(stale_backups))
            await async_remove_backups(stale_backups)
        else:
            _LOGGER.info('No backups found.')

    hass.services.async_register(DOMAIN, 'clean_up', async_handle_clean_up)

    return True
