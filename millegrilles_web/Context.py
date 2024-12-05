import datetime
import logging
import ssl
from asyncio import TaskGroup

from typing import Callable, Optional

from millegrilles_messages.bus.BusContext import MilleGrillesBusContext
from millegrilles_messages.bus.BusExceptions import ConfigurationFileError
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector
from millegrilles_messages.structs.Filehost import Filehost
from millegrilles_web.Configuration import WebAppConfiguration


class KeymasterCertificate:

    def __init__(self, fingerprint: str, pems: list[str]):
        self.__fingerprint = fingerprint
        self.__pems = pems
        self.__received = datetime.datetime.now()

    def touch(self):
        self.__received = datetime.datetime.now()

    @property
    def pems(self) -> list[str]:
        return self.__pems

    def is_expired(self):
        date_expired = datetime.datetime.now() - datetime.timedelta(minutes=15)
        return self.__received < date_expired


class WebAppContext(MilleGrillesBusContext):

    def __init__(self, configuration: WebAppConfiguration):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__reload_listeners: list[Callable[[], None]] = list()
        self.__web_ssl_context: Optional[ssl.SSLContext] = None
        super().__init__(configuration)

        self.__bus_connector: Optional[MilleGrillesPikaConnector] = None
        self.__filehost: Optional[Filehost] = None
        self.__keymaster_certificates: dict[str, KeymasterCertificate] = dict()

    @property
    def configuration(self) -> WebAppConfiguration:
        return super().configuration

    @property
    def web_ssl_context(self) -> ssl.SSLContext:
        return self.__web_ssl_context

    def reload(self):
        super().reload()
        self.__web_ssl_context = self.__load_ssl_context()
        for listener in self.__reload_listeners:
            listener()

    def __load_ssl_context(self):
        configuration = self.configuration

        if configuration.web_client_ssl_verification:
            # Using millegrille certificate
            return super().ssl_context

        # Loading a separate web key/cert
        self.__logger.debug("Load web certificate %s" % configuration.cert_path)
        ssl_context = ssl.SSLContext()
        try:
            web_cert_path = configuration.web_cert_path
            web_key_path = configuration.web_key_path
            ssl_context.load_cert_chain(web_cert_path, web_key_path)
        except FileNotFoundError:
            files = "%s or %s" % (configuration.web_cert_path, configuration.web_key_path)
            raise ConfigurationFileError(files)

        ssl_context.verify_mode = False

        return ssl_context

    def add_reload_listener(self, listener: Callable[[], None]):
        self.__reload_listeners.append(listener)

    @property
    def bus_connector(self):
        return self.__bus_connector

    @bus_connector.setter
    def bus_connector(self, value: MilleGrillesPikaConnector):
        self.__bus_connector = value

    async def get_producer(self):
        return await self.__bus_connector.get_producer()

    @property
    def filehost(self) -> Optional[Filehost]:
        return self.__filehost

    @filehost.setter
    def filehost(self, value: Optional[Filehost]):
        self.__filehost = value

    @property
    def keymaster_certificates(self) -> list[list[str]]:
        pems = list()
        for certificate in self.__keymaster_certificates.values():
            pems.append(certificate.pems)
        return pems

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(super().run())
            group.create_task(self.__maintain_certificates())

    async def __maintain_certificates(self):
        while self.stopping is False:
            remove_fingerprint = list()

            # Identify expired certificates
            for fingerprint, value in self.__keymaster_certificates.items():
                if value.is_expired():
                    remove_fingerprint.append(fingerprint)

            # Remove certificates
            for fingerprint in remove_fingerprint:
                del self.__keymaster_certificates[fingerprint]

            await self.wait(300)

    def update_keymaster_certificate(self, fingerprint: str, pem: list[str]):
        try:
            self.__keymaster_certificates[fingerprint].touch()
        except KeyError:
            self.__keymaster_certificates[fingerprint] = KeymasterCertificate(fingerprint, pem)
