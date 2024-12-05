import aiohttp_session
import asyncio
import logging
import pathlib

import aiohttp_session.redis_storage
import redis.asyncio

from aiohttp.web import StreamResponse
from redis.asyncio.client import Redis as RedisClient

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import TaskGroup
from typing import Optional

from millegrilles_messages.bus.BusContext import ForceTerminateExecution
from millegrilles_web import Constantes as ConstantesWeb
from millegrilles_web.WebAppManager import WebAppManager


class WebServer:
    """
    Web access module.
    """

    def __init__(self, manager: WebAppManager):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__manager = manager

        self._app = web.Application()

        self._semaphore_web_threads = asyncio.BoundedSemaphore(value=10)

    @property
    def web_app(self) -> web.Application:
        return self._app

    async def setup(self):
        await self._charger_session_handler()
        await self._prepare_routes()

    async def __on_prepare_caching(self, request: Request, response: StreamResponse):
        path_request = request.path
        if path_request.startswith(f'{self.__manager.application_path}/static'):
            response.headers.add('Cache-Control', 'public, max-age=86400, immutable')
        elif path_request.endswith('.css') or path_request.endswith('.json') or path_request.endswith('.ico'):
            response.headers.add('Cache-Control', 'public, max-age=600')

    async def serve_index_html(self, _request: Request):
        headers = {'Cache-Control': 'public, max-age=600'}
        return web.FileResponse(f'static{self.__manager.application_path}/index.html', headers=headers)

    async def _prepare_routes(self):
        self._app.on_response_prepare.append(self.__on_prepare_caching)
        app_path = self.__manager.application_path
        self._app.add_routes([
            web.get(f'{app_path}/initSession', self.handle_init_session),
            web.get(f'{app_path}/info.json', self.handle_info_session),
        ])

        path_static = pathlib.Path('static')
        if path_static.exists():
            self._app.router.add_get(f'{app_path}', self.serve_index_html)
            self._app.router.add_get(f'{app_path}/', self.serve_index_html)
            web.get(f'{app_path}/', self.serve_index_html),
            self._app.router.add_static(f'{app_path}', path=f'static{app_path}', name='react', append_version=True)
        else:
            self.__logger.warning('Directory static/ not available - operating without react application')

    async def _connect_redis(self, redis_database: Optional[int] = None) -> RedisClient:
        configuration_app = self.__manager.context.configuration
        redis_hostname = configuration_app.redis_hostname
        redis_port = configuration_app.redis_port
        redis_username = configuration_app.redis_username
        with open(configuration_app.redis_password_path, 'rt') as fp:
            redis_password = await asyncio.to_thread(fp.readline, 1024)
        key_path = configuration_app.key_path
        cert_path = configuration_app.cert_path
        ca_path = configuration_app.ca_path
        redis_database_val = redis_database or configuration_app.redis_session_db

        url_redis = f"rediss://{redis_hostname}:{redis_port}"

        self.__logger.info("Connecting to redis for web sessions : %s", url_redis)

        redis_session = await redis.asyncio.from_url(
            url_redis, db=redis_database_val, username=redis_username, password=redis_password,
            ssl_keyfile=key_path, ssl_certfile=cert_path, ssl_ca_certs=ca_path,
            ssl_cert_reqs="required", ssl_check_hostname=True,
        )

        return redis_session

    async def _charger_session_handler(self):
        redis_session = await self._connect_redis()

        # Check connection with redis - raise error if failing
        await redis_session.ping()

        app_name = self.__manager.app_name
        session_duration = self.__manager.context.configuration.session_duration

        storage = aiohttp_session.redis_storage.RedisStorage(
            redis_session, cookie_name=f'{app_name}.aiohttp',
            max_age=session_duration,
            secure=True, httponly=True
        )

        # Wiring of the session
        aiohttp_session.setup(self._app, storage)

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(self.__run_web_server())

    async def __run_web_server(self):
        runner = web.AppRunner(self._app)
        await runner.setup()

        # Configuration du site avec SSL
        port = self.__manager.context.configuration.port
        context = self.__manager.context
        ssl_context = context.web_ssl_context
        site = web.TCPSite(runner, '0.0.0.0', port, ssl_context=ssl_context)

        try:
            await site.start()
            self.__logger.info("Website %s started on port %d", self.__manager.application_path, port)
            await context.wait()  # Wait until application shuts down
        except:
            self.__logger.exception("Error running website - quitting")
            self.__manager.context.stop()
            raise ForceTerminateExecution()
        finally:
            self.__logger.info("Website stopped")
            await runner.cleanup()

    async def handle_init_session(self, request: Request):
        async with self._semaphore_web_threads:
            if {ConstantesWeb.HEADER_USER_NAME, ConstantesWeb.HEADER_USER_ID}.issubset(request.headers) is False:
                return web.HTTPUnauthorized()
            return web.HTTPOk()

    async def handle_info_session(self, request: Request):
        async with self._semaphore_web_threads:
            try:
                user_name = request.headers[ConstantesWeb.HEADER_USER_NAME]
                user_id = request.headers[ConstantesWeb.HEADER_USER_ID]
            except KeyError:
                return web.HTTPUnauthorized()

            data = {
                'nomUsager': user_name,
                'userId': user_id,
            }

            return web.json_response(data)
