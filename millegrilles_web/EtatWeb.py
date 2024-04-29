import asyncio
import logging

from typing import Optional

from ssl import SSLContext

from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_web.Configuration import ConfigurationApplicationWeb


class EtatWeb(EtatInstance):

    def __init__(self, configuration: ConfigurationApplicationWeb):
        super().__init__(configuration)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.__ssl_context: Optional[SSLContext] = None

        self.__url_consignation: Optional[str] = None
        self.__event_consignation = asyncio.Event()

    async def reload_configuration(self):
        await super().reload_configuration()
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(self.configuration.cert_pem_path, self.configuration.key_pem_path)

    @property
    def ssl_context(self):
        return self.__ssl_context

    @property
    def configuration(self) -> ConfigurationApplicationWeb:
        return super().configuration

    async def charger_consignation_thread(self, stop_event: asyncio.Event):
        while stop_event.is_set() is False:
            retry_timeout = 300

            try:
                url_consignation = await self.charger_consignation()
                self.__logger.info("charger_consignation_thread URL consignation : %s" % url_consignation)
            except Exception:
                self.__logger.exception("Erreur chargement consignation")
                retry_timeout = 30

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=retry_timeout)
            except asyncio.TimeoutError:
                pass  # OK

    async def charger_consignation(self):
        producer = self.producer
        if producer is None:
            await asyncio.sleep(5)  # Attendre connexion MQ
            producer = self.producer
            if producer is None:
                raise Exception('producer pas pret')
        await asyncio.wait_for(producer.producer_pret().wait(), 30)

        reponse = await producer.executer_requete(
            {}, 'CoreTopologie', 'getConsignationFichiers', exchange="1.public")

        try:
            consignation_url = reponse.parsed['consignation_url']
            self.__url_consignation = consignation_url
            self.__event_consignation.set()
            return consignation_url
        except Exception as e:
            self.__logger.exception("Erreur chargement URL consignation")

    async def get_url_consignation(self) -> str:
        await self.__event_consignation.wait()
        return self.__url_consignation
