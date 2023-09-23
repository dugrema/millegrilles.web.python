import aiohttp_session
import aioredis
import asyncio
import logging
import jwt
import socketio

import aiohttp_session.redis_storage
import redis.asyncio
from socketio.exceptions import ConnectionRefusedError

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event
from asyncio.exceptions import TimeoutError
from ssl import SSLContext
from typing import Optional, Union

from millegrilles_messages.messages import Constantes

from millegrilles_web import Constantes as ConstantesWeb
from millegrilles_web.Configuration import ConfigurationWeb
from millegrilles_web.EtatWeb import EtatWeb
from millegrilles_web.Commandes import CommandHandler
from millegrilles_web.SocketIoHandler import SocketIoHandler


class WebServer:

    def __init__(self, app_path: str, etat: EtatWeb, commandes: CommandHandler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__app_path = app_path
        self._etat = etat
        self.__commandes = commandes

        self._stop_event: Optional[Event] = None

        self._app = web.Application()
        self.__configuration = ConfigurationWeb()
        self.__ssl_context: Optional[SSLContext] = None
        self._redis_session: Optional[aioredis.Redis] = None
        self._socket_io_handler: Optional[SocketIoHandler] = None

        self._semaphore_web_threads = asyncio.BoundedSemaphore(value=10)

    @property
    def app(self):
        return self._app

    @property
    def etat(self):
        return self._etat

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
        # self._socket_io_handler = SocketIoHandler(self.__app, self.__etat)
        raise NotImplementedError('must implement')

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    async def _preparer_routes(self):
        self._app.add_routes([
            web.get(f'{self.__app_path}/initSession', self.handle_init_session),
            web.get(f'{self.__app_path}/info.json', self.handle_info_session)
        ])

    # def _preparer_socketio_events(self):
    #     self._sio.on("connect", handler=self.sio_connect)
    #     self._sio.on("disconnect", handler=self.sio_disconnect)

    def _charger_ssl(self):
        self.__ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % self.__configuration.web_cert_pem_path)
        self.__ssl_context.load_cert_chain(self.__configuration.web_cert_pem_path,
                                           self.__configuration.web_key_pem_path)

    async def _charger_session_handler(self):
        configuration_app = self._etat.configuration
        redis_hostname = configuration_app.redis_hostname
        redis_port = configuration_app.redis_port
        redis_username = configuration_app.redis_username
        redis_password = configuration_app.redis_password
        key_path = configuration_app.key_pem_path
        cert_path = configuration_app.cert_pem_path
        ca_path = configuration_app.ca_pem_path
        redis_database = configuration_app.redis_session_db

        url_redis = f"rediss://{redis_hostname}:{redis_port}"

        self.__logger.info("Connexion a redis pour session web : %s" % url_redis)

        redis_session = await redis.asyncio.from_url(
            url_redis, db=redis_database, username=redis_username, password=redis_password,
            ssl_keyfile=key_path, ssl_certfile=cert_path, ssl_ca_certs=ca_path,
            ssl_cert_reqs="required", ssl_check_hostname=True,
        )

        # Verifier connexion a redis - raise erreur si echec
        await redis_session.ping()

        storage = aiohttp_session.redis_storage.RedisStorage(
            redis_session, cookie_name=self.get_nom_app() + '.aiohttp',
            max_age=1800,
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
        await asyncio.gather(
            self.__run_web_server(),
            self.entretien(),
            self._socket_io_handler.run(),
        )

    async def __run_web_server(self):
        runner = web.AppRunner(self._app)
        await runner.setup()

        # Configuration du site avec SSL
        port = self.__configuration.port
        site = web.TCPSite(runner, '0.0.0.0', port, ssl_context=self.__ssl_context)

        try:
            await site.start()
            self.__logger.info("Site demarre")

            await self._stop_event.wait()
            # while not self._stop_event.is_set():
            #     await self.entretien()
            #     try:
            #         await asyncio.wait_for(self._stop_event.wait(), 30)
            #     except TimeoutError:
            #         pass
        finally:
            self.__logger.info("Site arrete")
            await runner.cleanup()

    # async def verifier_token_jwt(self, token: str, fuuid: str) -> Union[bool, dict]:
    #     # Recuperer kid, charger certificat pour validation
    #     header = jwt.get_unverified_header(token)
    #     fingerprint = header['kid']
    #     enveloppe = await self.__etat.charger_certificat(fingerprint)
    #
    #     domaines = enveloppe.get_domaines
    #     if 'GrosFichiers' in domaines or 'Messagerie' in domaines:
    #         pass  # OK
    #     else:
    #         # Certificat n'est pas autorise a signer des streams
    #         self.__logger.warning("Certificat de mauvais domaine pour JWT (doit etre GrosFichiers,Messagerie)")
    #         return False
    #
    #     exchanges = enveloppe.get_exchanges
    #     if Constantes.SECURITE_SECURE not in exchanges:
    #         # Certificat n'est pas autorise a signer des streams
    #         self.__logger.warning("Certificat de mauvais niveau de securite pour JWT (doit etre 4.secure)")
    #         return False
    #
    #     public_key = enveloppe.get_public_key()
    #
    #     try:
    #         claims = jwt.decode(token, public_key, algorithms=['EdDSA'])
    #     except jwt.exceptions.InvalidSignatureError:
    #         # Signature invalide
    #         return False
    #
    #     self.__logger.debug("JWT claims pour %s = %s" % (fuuid, claims))
    #
    #     if claims['sub'] != fuuid:
    #         # JWT pour le mauvais fuuid
    #         return False
    #
    #     return claims

    async def handle_init_session(self, request: Request):
        async with self._semaphore_web_threads:
            try:
                user_name = request.headers[ConstantesWeb.HEADER_USER_NAME]
                user_id = request.headers[ConstantesWeb.HEADER_USER_ID]
            except KeyError:
                return web.HTTPUnauthorized()

            # session = await aiohttp_session.get_session(request)

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

    # async def sio_connect(self, sid, environ):
    #     self.__logger.debug("connect %s", sid)
    #     try:
    #         request = environ.get('aiohttp.request')
    #         user_id = request.headers[ConstantesWeb.HEADER_USER_ID]
    #         user_name = request.headers[ConstantesWeb.HEADER_USER_NAME]
    #     except KeyError:
    #         self.__logger.error("sio_connect SID:%s sans parametres request user_id/user_name (pas de session)" % sid)
    #         raise ConnectionRefusedError('authentication failed')
    #
    #     async with self._sio.session(sid) as session:
    #         session['user_name'] = user_name
    #         session['user_id'] = user_id
    #
    #     return True
    #
    # async def sio_disconnect(self, sid):
    #     self.__logger.debug("disconnect %s", sid)
