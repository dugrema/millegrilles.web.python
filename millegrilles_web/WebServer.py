import asyncio
import logging
import jwt
import pathlib
import re

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event
from asyncio.exceptions import TimeoutError
from ssl import SSLContext
from typing import Optional, Union

from millegrilles_messages.messages import Constantes

from millegrilles_web.Configuration import ConfigurationWeb
from millegrilles_web.EtatWeb import EtatWeb
from millegrilles_web.Commandes import CommandHandler


class WebServer:

    def __init__(self, etat: EtatWeb, commandes: CommandHandler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat = etat
        self.__commandes = commandes

        self.__app = web.Application()
        self.__stop_event: Optional[Event] = None
        self.__configuration = ConfigurationWeb()
        self.__ssl_context: Optional[SSLContext] = None

    def setup(self, configuration: Optional[dict] = None):
        self._charger_configuration(configuration)
        self._preparer_routes()
        self._charger_ssl()

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    def _preparer_routes(self):
        raise NotImplementedError('must implement')

    def _charger_ssl(self):
        self.__ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % self.__configuration.web_cert_pem_path)
        self.__ssl_context.load_cert_chain(self.__configuration.web_cert_pem_path,
                                           self.__configuration.web_key_pem_path)

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
