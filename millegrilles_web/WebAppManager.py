import logging

from typing import Callable, Awaitable, Optional

from millegrilles_messages.bus.PikaQueue import MilleGrillesPikaQueueConsumer
from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.structs.Filehost import Filehost
from millegrilles_web.Context import WebAppContext


class WebAppManager:
    """
    Facade for access modules (web, bus)
    """

    def __init__(self, context: WebAppContext):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context

        self.__filehost_listeners: list[Callable[[Optional[Filehost]], Awaitable[None]]] = list()

    @property
    def context(self) -> WebAppContext:
        return self.__context

    def add_filehost_listener(self, listener: Callable[[Optional[Filehost]], Awaitable[None]]):
        self.__filehost_listeners.append(listener)

    async def __reload_filehost_thread(self):
        while self.__context.stopping is False:
            try:
                await self.reload_filehost_configuration()
                await self.__context.wait(900)
            except:
                self.__logger.exception("Error loading filehost configuration")
                await self.__context.wait(30)

    async def reload_filehost_configuration(self):
        producer = await self.__context.get_producer()
        response = await producer.request(
            dict(), 'CoreTopologie', 'getFilehostForInstance', exchange="1.public")

        try:
            filehost_response = response.parsed
            filehost_dict = filehost_response['filehost']
            filehost = Filehost.load_from_dict(filehost_dict)
            self.__context.filehost = filehost
        except:
            self.__logger.exception("Error loading filehost")
            self.__context.filehost = None

        for l in self.__filehost_listeners:
            await l(self.__context.filehost)

    @property
    def app_name(self) -> str:
        raise NotImplementedError('must implement')

    @property
    def application_path(self):
        return self.__context.configuration.application_path or f'/{self.app_name}'

    async def update_keymaster_certificate(self, message: MessageWrapper):
        certificat = message.certificat
        if Constantes.ROLE_MAITRE_DES_CLES in certificat.get_roles:
            fingerprint = certificat.fingerprint
            pem = certificat.chaine_pem()
            self.__context.update_keymaster_certificate(fingerprint, pem)

    def get_subcription_queue(self) -> MilleGrillesPikaQueueConsumer:
        raise NotImplementedError('not available')

    async def evict_user(self, message: MessageWrapper):
        raise NotImplementedError('not available')
