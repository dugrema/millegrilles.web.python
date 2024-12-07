from typing import Callable, Awaitable, Optional

from millegrilles_messages.bus.PikaQueue import MilleGrillesPikaQueueConsumer
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_web.WebAppManager import WebAppManager
from millegrilles_web.mapped.MappedContext import MappedWebAppContext


class MappedWebAppManager(WebAppManager):

    def __init__(self, context: MappedWebAppContext):
        super().__init__(context)
        self.__subscription_callback: Optional[Callable[[MessageWrapper], Awaitable[None]]] = None
        self.__get_subscription_queue: Optional[Callable[[], MilleGrillesPikaQueueConsumer]] = None
        self.__evict_user_callback: Optional[Callable[[str], Awaitable[None]]] = None

    @property
    def app_name(self) -> str:
        return 'millegrilles'

    @property
    def application_path(self):
        return '/millegrilles'

    def setup(self,
              # subscription_callback: Callable[[MessageWrapper], Awaitable[None]],
              get_subscription_queue: Callable[[], MilleGrillesPikaQueueConsumer],
              evict_user_callback: Callable[[str], Awaitable[None]]):
        # self.__subscription_callback = subscription_callback
        self.__get_subscription_queue = get_subscription_queue
        self.__evict_user_callback = evict_user_callback

    # async def subscription_callback_handler(self, message: MessageWrapper):
    #     await self.__subscription_callback(message)

    def get_subscription_queue(self) -> MilleGrillesPikaQueueConsumer:
        return self.__get_subscription_queue()

    async def evict_user(self, message: MessageWrapper):
        user_id = message.parsed['userId']
        self.__logger.info("evict_user Evict user %s", user_id)
        await self.__evict_user_callback(user_id)
