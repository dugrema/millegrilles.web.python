import asyncio
import logging

from typing import Optional

from ssl import SSLContext
from urllib.parse import urljoin

from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_web.Configuration import ConfigurationApplicationWeb


class InformationFilehost:

    def __init__(self, filehost: dict, url: str, filehost_id: str, instance_id: str):
        self.__filehost = filehost
        self.url = url
        self.instance_id = instance_id
        self.filehost_id = filehost_id

    @staticmethod
    def from_filehost(filehost: dict, local_instance_id: str):
        filehost_id = filehost['filehost_id']
        if filehost.get('instance_id') and filehost.get('url_internal'):
            # Use local nginx passthrough. Localhost is placeholder for connection that client is using.
            url = f'https://localhost/filehost/'
        elif filehost.get('url_external') and filehost.get('tls_external') in ['nocheck', 'external']:
            # Use external URL (internet)
            url = urljoin(filehost['url_external'], '/filehost/')
        else:
            raise Exception('Filehost not available externally')

        info = InformationFilehost(filehost, url, filehost_id, local_instance_id)

        return info

    def to_dict(self):
        return {
            'url': self.url,
            'filehost_id': self.filehost_id,
            'instance_id': self.instance_id,
        }


class EtatWeb(EtatInstance):

    def __init__(self, configuration: ConfigurationApplicationWeb):
        super().__init__(configuration)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.__ssl_context: Optional[SSLContext] = None

        # self.__url_consignation: Optional[str] = None
        # self.__consignation: Optional[InformationFilehost] = None
        self.__filehost: Optional[InformationFilehost] = None
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
        producer = None
        for i in range(0, 6):
            producer = self.producer
            if producer:
                break
            await asyncio.sleep(0.5)  # Attendre connexion MQ

        if producer is None:
            raise Exception('producer pas pret')

        reponse = await producer.executer_requete(
            {}, 'CoreTopologie', 'getFilehostForInstance', exchange="1.public")

        try:
            filehost_reponse = reponse.parsed
            if filehost_reponse['ok'] is not True:
                raise Exception('information filehost non disponible')

            filehost = filehost_reponse['filehost']
            # url_internal = filehost_reponse.get('url_internal')
            # url_exnternal = filehost_reponse.get('url_internal')
            # tls_external = filehost_reponse.get('url_internal')
            # filehost_instance_id = filehost_reponse.get('instance_id')

            # consignation_url = reponse.parsed['consignation_url']
            # type_store = reponse.parsed['type_store']
            # instance_id = reponse.parsed['instance_id']
            # self.__url_consignation = consignation_url
            # self.__consignation = InformationFilehost(consignation_url, type_store, instance_id)
            self.__filehost = InformationFilehost.from_filehost(filehost, self.instance_id)
            self.__event_consignation.set()
        except Exception as e:
            self.__logger.exception("Erreur chargement URL consignation")

    async def get_url_consignation(self) -> str:
        raise NotImplementedError('obsolete')
        # await self.__event_consignation.wait()
        # return self.__url_consignation

    async def get_filehost(self) -> InformationFilehost:
        await self.__event_consignation.wait()
        return self.__filehost

