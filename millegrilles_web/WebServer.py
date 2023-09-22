import aiohttp_session
import aioredis
import asyncio
import logging
import jwt

import aiohttp_session.redis_storage
import redis.asyncio

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


class WebServer:

    def __init__(self, app_path: str, etat: EtatWeb, commandes: CommandHandler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__app_path = app_path
        self.__etat = etat
        self.__commandes = commandes

        self.__app = web.Application()
        self.__stop_event: Optional[Event] = None
        self.__configuration = ConfigurationWeb()
        self.__ssl_context: Optional[SSLContext] = None
        self._redis_session: Optional[aioredis.Redis] = None

    def get_nom_app(self) -> str:
        raise NotImplementedError('must implement')

    async def setup(self, configuration: Optional[dict] = None):
        self._charger_configuration(configuration)
        await self._charger_session_handler()
        self._charger_ssl()

        self._preparer_routes()

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    def _preparer_routes(self):
        self.__app.add_routes([
            web.get(f'{self.__app_path}/initSession', self.handle_init_session),
            web.get(f'{self.__app_path}/info.json', self.handle_info_session)
        ])

    def _charger_ssl(self):
        self.__ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % self.__configuration.web_cert_pem_path)
        self.__ssl_context.load_cert_chain(self.__configuration.web_cert_pem_path,
                                           self.__configuration.web_key_pem_path)

    async def _charger_session_handler(self):
        configuration_app = self.__etat.configuration
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

        await redis_session.ping()

        if isinstance(redis_session, redis.asyncio.Redis):
            pass

        storage = aiohttp_session.redis_storage.RedisStorage(
            redis_session, cookie_name=self.get_nom_app() + '.web.sid',
            max_age=1800,
            secure=True, httponly=True
        )

        # Wiring de la session dans webapp
        aiohttp_session.setup(self.__app, storage)

    async def entretien(self):
        self.__logger.debug('Entretien web')

    async def run(self, stop_event: Optional[Event] = None):
        if stop_event is not None:
            self.__stop_event = stop_event
        else:
            self.__stop_event = Event()

        runner = web.AppRunner(self.__app)
        await runner.setup()

        # Configuration du site avec SSL
        port = self.__configuration.port
        site = web.TCPSite(runner, '0.0.0.0', port, ssl_context=self.__ssl_context)

        try:
            await site.start()
            self.__logger.info("Site demarre")

            while not self.__stop_event.is_set():
                await self.entretien()
                try:
                    await asyncio.wait_for(self.__stop_event.wait(), 30)
                except TimeoutError:
                    pass
        finally:
            self.__logger.info("Site arrete")
            await runner.cleanup()

    async def verifier_token_jwt(self, token: str, fuuid: str) -> Union[bool, dict]:
        # Recuperer kid, charger certificat pour validation
        header = jwt.get_unverified_header(token)
        fingerprint = header['kid']
        enveloppe = await self.__etat.charger_certificat(fingerprint)

        domaines = enveloppe.get_domaines
        if 'GrosFichiers' in domaines or 'Messagerie' in domaines:
            pass  # OK
        else:
            # Certificat n'est pas autorise a signer des streams
            self.__logger.warning("Certificat de mauvais domaine pour JWT (doit etre GrosFichiers,Messagerie)")
            return False

        exchanges = enveloppe.get_exchanges
        if Constantes.SECURITE_SECURE not in exchanges:
            # Certificat n'est pas autorise a signer des streams
            self.__logger.warning("Certificat de mauvais niveau de securite pour JWT (doit etre 4.secure)")
            return False

        public_key = enveloppe.get_public_key()

        try:
            claims = jwt.decode(token, public_key, algorithms=['EdDSA'])
        except jwt.exceptions.InvalidSignatureError:
            # Signature invalide
            return False

        self.__logger.debug("JWT claims pour %s = %s" % (fuuid, claims))

        if claims['sub'] != fuuid:
            # JWT pour le mauvais fuuid
            return False

        return claims

    async def handle_init_session(self, request: Request):

        try:
            user_name = request.headers[ConstantesWeb.HEADER_USER_NAME]
            user_id = request.headers[ConstantesWeb.HEADER_USER_ID]
        except KeyError:
            return web.HTTPUnauthorized()

        # session = await aiohttp_session.get_session(request)

        return web.HTTPOk()

    async def handle_info_session(self, request: Request):

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
