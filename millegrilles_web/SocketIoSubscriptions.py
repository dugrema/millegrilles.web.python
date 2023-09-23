import asyncio
import logging

from typing import Optional, Union


class SocketIoSubscriptions:
    """
    Gere les subscriptions a des "chat rooms" pour recevoir des evenements MQ via Socket.IO
    """

    def __init__(self, sio_handler, stop_event: asyncio.Event):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__sio_handler = sio_handler
        self.__stop_event = stop_event

    async def setup(self):
        pass

    async def run(self):
        self.__logger.info("run Debut")
        await self.__stop_event.wait()
        self.__logger.info("run Fin")

    async def subscribe(self, sid: str, routing_keys: Union[str, list[str]], exchanges: Union[str, list[str]]):
        pass

    async def unsubscribe(self, sid: str, routing_keys: Union[str, list[str]], exchanges: Union[str, list[str]]):
        pass
