import aiohttp_session
import asyncio
import logging
import pathlib

import aiohttp_session.redis_storage
import redis.asyncio

from redis.asyncio.client import Redis as RedisClient

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event, TaskGroup
from ssl import SSLContext
from typing import Optional

from millegrilles_web import Constantes as ConstantesWeb
from millegrilles_web.Configuration import ConfigurationWeb, ConfigurationApplicationWeb
from millegrilles_web.EtatWeb import EtatWeb
from millegrilles_web.Commandes import CommandHandler
from millegrilles_web.SocketIoHandler import SocketIoHandler


class WebServer:

    def __init__(self, app_path: str, etat: EtatWeb, commandes: CommandHandler, duree_session=1800):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__app_path = app_path
        self._etat = etat
        self._commandes = commandes
        self.__duree_session = duree_session

        self._stop_event: Optional[Event] = None

        self._app = web.Application()
        self.__configuration = ConfigurationWeb()
        self.__ssl_context: Optional[SSLContext] = None
        # self._redis_session: Optional[aioredis.Redis] = None
        self._socket_io_handler: Optional[SocketIoHandler] = None

        self._semaphore_web_threads = asyncio.BoundedSemaphore(value=10)

    @property
    def app(self):
        return self._app

    @property
    def etat(self):
        return self._etat

    @property
    def app_path(self):
        return self.__app_path

    @property
    def socket_io_handler(self):
        return self._socket_io_handler

    def get_nom_app(self) -> str:
        raise NotImplementedError('must implement')

    async def setup(self, configuration: Optional[dict] = None, stop_event: Optional[asyncio.Event] = None):
        if stop_event is not None:
            self._stop_event = stop_event
        else:
            self._stop_event = asyncio.Event()

        self._charger_configuration(configuration)
        await self._charger_session_handler()
        self._charger_ssl()
        await self.setup_socketio()
        await self._preparer_routes()

    async def setup_socketio(self):
        """ Wiring socket.io """
        # Utiliser la bonne instance de SocketIoHandler dans une sous-classe
        raise NotImplementedError('must implement')

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    async def on_prepare_caching(self, request, response):
        path_request = request.path
        if path_request.startswith(f'/{self.get_nom_app()}/static'):
            response.headers.add('Cache-Control', 'public, max-age=86400, immutable')
        elif path_request.endswith('.css') or path_request.endswith('.json') or path_request.endswith('.ico'):
            response.headers.add('Cache-Control', 'public, max-age=600')

    async def serve_index_html(self, request: Request):
        headers = {'Cache-Control': 'public, max-age=600'}
        return web.FileResponse(f'static/{self.get_nom_app()}/index.html', headers=headers)

    async def _preparer_routes(self):
        self._app.on_response_prepare.append(self.on_prepare_caching)
        nom_app = self.get_nom_app()
        self._app.add_routes([
            web.get(f'{self.__app_path}/initSession', self.handle_init_session),
            web.get(f'{self.__app_path}/info.json', self.handle_info_session),
        ])

        path_static = pathlib.Path('static')
        if path_static.exists():
            self._app.router.add_get(f'/{nom_app}', self.serve_index_html)
            self._app.router.add_get(f'/{nom_app}/', self.serve_index_html)
            web.get(f'/{nom_app}/', self.serve_index_html),
            self._app.router.add_static(f'/{nom_app}', path=f'static/{nom_app}', name='react', append_version=True)
        else:
            self.__logger.warning('Repertoire static/ non disponible - mode sans application react')

    def _charger_ssl(self):
        self.__ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % self.__configuration.web_cert_pem_path)
        self.__ssl_context.load_cert_chain(self.__configuration.web_cert_pem_path,
                                           self.__configuration.web_key_pem_path)

    async def _connect_redis(self, redis_database: Optional[int] = None) -> RedisClient:
        configuration_app: ConfigurationApplicationWeb = self._etat.configuration
        redis_hostname = configuration_app.redis_hostname
        redis_port = configuration_app.redis_port
        redis_username = configuration_app.redis_username
        redis_password = configuration_app.redis_password
        key_path = configuration_app.key_pem_path
        cert_path = configuration_app.cert_pem_path
        ca_path = configuration_app.ca_pem_path
        redis_database_val = redis_database or configuration_app.redis_session_db

        url_redis = f"rediss://{redis_hostname}:{redis_port}"

        self.__logger.info("Connexion a redis pour session web : %s" % url_redis)

        redis_session = await redis.asyncio.from_url(
            url_redis, db=redis_database_val, username=redis_username, password=redis_password,
            ssl_keyfile=key_path, ssl_certfile=cert_path, ssl_ca_certs=ca_path,
            ssl_cert_reqs="required", ssl_check_hostname=True,
        )

        return redis_session

    async def _charger_session_handler(self):
        redis_session = await self._connect_redis()

        # Verifier connexion a redis - raise erreur si echec
        await redis_session.ping()

        storage = aiohttp_session.redis_storage.RedisStorage(
            redis_session, cookie_name=self.get_nom_app() + '.aiohttp',
            max_age=self.__duree_session,
            secure=True, httponly=True
        )

        # Wiring de la session dans webapp
        aiohttp_session.setup(self._app, storage)

    async def entretien(self):
        while self._stop_event.is_set() is False:
            self.__logger.debug('Entretien web')

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass  # OK

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(self.__run_web_server())
            group.create_task(self.entretien())
            if self._socket_io_handler is not None:
                group.create_task(self._socket_io_handler.run())
            else:
                self.__logger.warning('socket.io non initialise')

    async def __run_web_server(self):
        runner = web.AppRunner(self._app)
        await runner.setup()

        # Configuration du site avec SSL
        port = self.__configuration.port
        site = web.TCPSite(runner, '0.0.0.0', port, ssl_context=self.__ssl_context)

        try:
            await site.start()
            self.__logger.info("Site /%s demarre" % self.get_nom_app())
            await self._stop_event.wait()
        finally:
            self.__logger.info("Site /%s arrete" % self.get_nom_app())
            await runner.cleanup()

    async def handle_init_session(self, request: Request):
        async with self._semaphore_web_threads:
            try:
                user_name = request.headers[ConstantesWeb.HEADER_USER_NAME]
                user_id = request.headers[ConstantesWeb.HEADER_USER_ID]
            except KeyError:
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
