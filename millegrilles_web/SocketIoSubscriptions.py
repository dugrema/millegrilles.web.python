import asyncio
import logging

from typing import Optional, Union

from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.messages.MessagesThread import MessagesThread, RessourcesConsommation


class SocketIoSubscriptions:
    """
    Gere les subscriptions a des "chat rooms" pour recevoir des evenements MQ via Socket.IO
    """

    def __init__(self, sio_handler, stop_event: asyncio.Event):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__sio_handler = sio_handler
        self.__stop_event = stop_event
        self.__messages_thread: Optional[MessagesThread] = None
        self.__ressources_consommation: Optional[RessourcesConsommation] = None

        self.__rooms = dict()  # { [room_handle]: {exchange, routing_key} }
        self.__semaphore_rooms = asyncio.BoundedSemaphore(value=1)

    @property
    def messages_thread(self) -> Optional[MessagesThread]:
        return self.__messages_thread

    @messages_thread.setter
    def messages_thread(self, m):
        self.__messages_thread = m

    @property
    def ressources_consommation(self) -> Optional[RessourcesConsommation]:
        return self.__ressources_consommation

    @ressources_consommation.setter
    def ressources_consommation(self, r):
        self.__ressources_consommation = r

    async def setup(self):
        pass

    async def run(self):
        self.__logger.info("run Debut")
        await asyncio.gather(
            self.entretien_rooms()
        )
        self.__logger.info("run Fin")

    async def entretien_rooms(self):
        while self.__stop_event.is_set() is False:

            rooms = self.__sio_handler.rooms
            namespace_root = rooms.get('/')

            # Faire entretien des rooms
            async with self.__semaphore_rooms:
                for room_handle, value in self.__rooms.copy().items():
                    if namespace_root is None or namespace_root.get(room_handle) is None:
                        # Retirer la routing key
                        await self.retirer_rk(**value)
                        del self.__rooms[room_handle]

            try:
                await asyncio.wait_for(self.__stop_event.wait(), timeout=20)
            except asyncio.TimeoutError:
                pass  # OK

    async def disconnect(self, sid: str):
        pass

    async def subscribe(self, sid: str, routing_keys: Union[str, list[str]], exchanges: Union[str, list[str]]):
        if isinstance(routing_keys, str):
            routing_keys = [routing_keys]
        if isinstance(exchanges, str):
            exchanges = [exchanges]

        for exchange in exchanges:
            for rk in routing_keys:
                await self.ajouter_sid_a_room(sid, exchange, rk)

        # {ok: true, routingKeys: rkEvents, exchanges, roomParam}
        return {
            'ok': True,
            'routingKeys': routing_keys, 'exchanges': exchanges,
            'roomParam': None
        }

    async def unsubscribe(self, sid: str, routing_keys: Union[str, list[str]], exchanges: Union[str, list[str]]):
        if isinstance(routing_keys, str):
            routing_keys = [routing_keys]
        if isinstance(exchanges, str):
            exchanges = [exchanges]

        for exchange in exchanges:
            for rk in routing_keys:
                await self.retirer_sid_de_room(sid, exchange, rk)

        # {ok: true, routingKeys: rkEvents, exchanges}
        return {'ok': True, 'routingKeys': routing_keys, 'exchanges': exchanges}

    async def ajouter_sid_a_room(self, sid: str, exchange: str, routing_key: str):
        room_handle = get_room_handle(exchange, routing_key)
        await self.ajouter_rk(routing_key, exchange)
        self.__sio_handler.ajouter_sid_room(sid, room_handle)

        # Conserver une reference pour cleanup de la routing key dans MQ si la room est fermee
        async with self.__semaphore_rooms:
            self.__rooms[room_handle] = {'exchange': exchange, 'routing_key': routing_key}

    async def retirer_sid_de_room(self, sid: str, exchange: str, routing_key: str):
        room_handle = get_room_handle(exchange, routing_key)
        self.__sio_handler.retirer_sid_room(sid, room_handle)

    async def callback_reply_q(self, message: MessageWrapper, module_messages: MessagesThread):
        self.__logger.debug("callback_reply_q RabbitMQ nessage recu : %s" % message)

        type_evenement, domaine, partition, action = get_key_parts(message.routing_key)
        message_room = {
            'exchange': message.exchange,
            'routingKey': message.routing_key,
            'message': message.parsed['__original']
        }
        exchange = message.exchange
        nom_evenement = f'{type_evenement}.{domaine}.{action}'

        # Toujours emettre sur la room avec partition __NONE__
        nom_room = f'{exchange}/{type_evenement}.{domaine}.__NONE__.{action}'
        await self.__sio_handler.emettre_message_room(nom_evenement, message_room, nom_room)

        if partition is not None:
            # Aussi emettre sur la room de la partition et sur partition __ALL__ (*)
            nom_room = f'{exchange}/{type_evenement}.{domaine}.{partition}.{action}'
            await self.__sio_handler.emettre_message_room(nom_evenement, message_room, nom_room)
            nom_room = f'{exchange}/{type_evenement}.{domaine}.__ALL__.{action}'
            await self.__sio_handler.emettre_message_room(nom_evenement, message_room, nom_room)

    async def ajouter_rk(self, routing_key: str, exchange: str):
        """ Ajouter une routing_key sur la Q du consumer dans MQ """
        messages_module = self.__messages_thread.messages_module
        consumers = messages_module.get_consumers()

        for consumer in consumers:
            if consumer.q == self.ressources_consommation.q:
                self.__logger.info("Queue %s : Ajouter rks %s sur exchanges %s" % (consumer.q, routing_key, exchange))
                consumer.ajouter_routing_key(exchange, routing_key)

    async def retirer_rk(self, routing_key: str, exchange: str):
        """ Retirer une routing_key de la Q du consumer dans MQ """
        messages_module = self.__messages_thread.messages_module
        consumers = messages_module.get_consumers()

        for consumer in consumers:
            if consumer.q == self.ressources_consommation.q:
                self.__logger.info("Queue %s : Retirer rks %s sur exchanges %s" % (consumer.q, routing_key, exchange))
                consumer.retirer_routing_key(exchange, routing_key)


def get_key_parts(routing_key: str):
    parts = routing_key.split('.')
    if len(parts) == 3:
        type_message, domaine, action = parts
        partition = '__NONE__'
    elif len(parts) == 4:
        type_message, domaine, partition, action = parts
    else:
        raise ValueError("Type de routing key non supporte : %s" % routing_key)

    if partition == '*':
        partition = '__ALL__'

    return type_message, domaine, partition, action


def get_room_handle(exchange: str, routing_key: str):
    type_message, domaine, partition, action = get_key_parts(routing_key)
    return f'{exchange}/{type_message}.{domaine}.{partition}.{action}'
